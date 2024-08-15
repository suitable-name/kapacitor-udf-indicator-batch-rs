use super::{
    config::{IndicatorOptions, IndicatorState, IndicatorType},
    indicators::Indicator,
};
use crate::handler::indicators::{ema::Ema, sma::Sma};
use async_std::{channel::Sender, sync::Mutex};
use async_trait::async_trait;
use kapacitor_udf::{
    proto::{
        response, BeginBatch, EdgeType, EndBatch, InfoResponse, InitRequest, InitResponse, Point,
        Response, RestoreRequest, RestoreResponse, SnapshotResponse,
    },
    traits::Handler,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, sync::Arc};
use thiserror::Error;
use tracing::{debug, error, instrument, trace, warn};

#[derive(Debug, Error)]
pub enum IndicatorError {
    #[error("Failed to send response: {0}")]
    ResponseSendError(String),
    #[error("Invalid field type: expected double, got {0}")]
    InvalidFieldType(String),
    #[error("Missing ticker field: {0}")]
    MissingTickerField(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndicatorData {
    states: HashMap<String, IndicatorState>,
    #[serde(skip)]
    batch_points: Vec<Point>,
}

pub struct IndicatorHandler {
    responses: Arc<Mutex<Sender<Response>>>,
    options: IndicatorOptions,
    data: IndicatorData,
    indicator: Box<dyn Indicator + Send>,
    begin_batch: Option<BeginBatch>,
}

impl IndicatorHandler {
    #[instrument(skip(responses, options))]
    pub async fn new(responses: Arc<Mutex<Sender<Response>>>, options: IndicatorOptions) -> Self {
        debug!("Creating new IndicatorHandler");

        let indicator: Box<dyn Indicator + Send> = match options.indicator_type {
            IndicatorType::EMA => Box::new(Ema),
            IndicatorType::SMA => Box::new(Sma),
        };

        IndicatorHandler {
            responses,
            options,
            data: IndicatorData {
                states: HashMap::new(),
                batch_points: Vec::new(),
            },
            indicator,
            begin_batch: None,
        }
    }

    #[instrument(skip(self))]
    async fn calculate_indicator(
        &mut self,
        ticker: &str,
        value: f64,
    ) -> Result<f64, IndicatorError> {
        debug!(
            "Calculating indicator for ticker: {}, value: {}",
            ticker, value
        );

        let state = self
            .data
            .states
            .entry(ticker.to_string())
            .or_insert_with(|| {
                debug!("Initializing new state for ticker: {}", ticker);
                IndicatorState {
                    current_value: None,
                    values: Vec::new(),
                    count: 0,
                }
            });

        debug!("State before calculation: {:?}", state);

        let result = self
            .indicator
            .calculate(state, self.options.period.try_into().unwrap(), value)
            .await;

        debug!(
            "Calculated result for ticker: {}, input: {}, output: {}, indicator type: {:?}",
            ticker, value, result, self.options.indicator_type
        );

        debug!("State after calculation: {:?}", state);

        Ok(result)
    }

    async fn send_response(&self, response: Response) -> Result<(), IndicatorError> {
        debug!("Sending response: {:?}", response);

        let sender = self.responses.lock().await;
        match sender.send(response).await {
            Ok(_) => {
                debug!("Response sent successfully");
                Ok(())
            }
            Err(e) => {
                error!("Failed to send response: {}", e);
                Err(IndicatorError::ResponseSendError(e.to_string()))
            }
        }
    }
}

#[async_trait]
impl Handler for IndicatorHandler {
    #[instrument(skip(self))]
    async fn info(&self) -> io::Result<InfoResponse> {
        debug!("Info request received");
        let info = InfoResponse {
            wants: EdgeType::Batch.into(),
            provides: EdgeType::Batch.into(),
            options: self.options.to_option_info(),
        };
        trace!("Responding with info: {:?}", info);
        Ok(info)
    }

    #[instrument(skip(self, r))]
    async fn init(&mut self, r: &InitRequest) -> io::Result<InitResponse> {
        debug!("Init request received: {:?}", r);
        match IndicatorOptions::from_proto_options(&r.options) {
            Ok(options) => {
                self.options = options;
                self.data.states.clear();
                self.data.batch_points.clear();
                Ok(InitResponse {
                    success: true,
                    error: String::new(),
                })
            }
            Err(e) => {
                error!("Failed to initialize: {}", e);
                Ok(InitResponse {
                    success: false,
                    error: e.to_string(),
                })
            }
        }
    }

    #[instrument(skip(self))]
    async fn snapshot(&self) -> io::Result<SnapshotResponse> {
        debug!("Snapshot request received");
        let snapshot = serde_json::to_vec(&self.data).map_err(|e| {
            error!("Failed to serialize state: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;
        Ok(SnapshotResponse { snapshot })
    }

    #[instrument(skip(self, req))]
    async fn restore(&mut self, req: &RestoreRequest) -> io::Result<RestoreResponse> {
        debug!("Restore request received");
        match serde_json::from_slice(&req.snapshot) {
            Ok(data) => {
                self.data = data;
                self.data.batch_points.clear(); // Clear batch points on restore
                Ok(RestoreResponse {
                    success: true,
                    error: String::new(),
                })
            }
            Err(e) => {
                error!("Failed to restore state: {}", e);
                Ok(RestoreResponse {
                    success: false,
                    error: e.to_string(),
                })
            }
        }
    }

    #[instrument(skip(self, begin))]
    async fn begin_batch(&mut self, begin: &BeginBatch) -> io::Result<()> {
        debug!("BeginBatch called: {:?}", begin);

        // Store BeginBatch for later use
        self.begin_batch = Some(begin.clone());

        // Reset state for new batch
        self.data.batch_points.clear();

        debug!("State reset for new batch");
        debug!("Sending EndBatch response");
        self.send_response(Response {
            message: Some(response::Message::Begin(begin.clone())),
        })
        .await
        .map_err(|e| {
            error!("Failed to send BeginBatch response: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        Ok(())
    }

    #[instrument(skip(self, p), fields(point_name = %p.name, point_time = %p.time))]
    async fn point(&mut self, p: &Point) -> io::Result<()> {
        debug!("Processing point: {:?}", p);

        if let Some(ticker) = p.tags.get(&self.options.ticker_field) {
            if let Some(value) = p.fields_double.get(&self.options.field) {
                debug!("Valid point data - ticker: {}, value: {}", ticker, value);
                self.data.batch_points.push(p.clone());
                debug!(
                    "Added point to batch. Current batch size: {}",
                    self.data.batch_points.len()
                );
            } else {
                warn!("Missing value for ticker: {}", ticker);
            }
        } else {
            warn!("Missing ticker in point tags");
        }

        Ok(())
    }

    #[instrument(skip(self, end))]
    async fn end_batch(&mut self, end: &EndBatch) -> io::Result<()> {
        debug!("EndBatch called: {:?}", end);

        // Validate that there are batch points to process, implying that BeginBatch was sent
        if self.data.batch_points.is_empty() {
            error!("Attempted to send EndBatch without any points being processed. Ensure BeginBatch was sent.");
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "EndBatch called without BeginBatch.",
            ));
        }

        debug!("Sending beginBatch");
        self.send_response(Response {
            message: Some(response::Message::Begin(self.begin_batch.clone().unwrap())),
        })
        .await
        .map_err(|e| {
            error!("Failed to send point response: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        // Collect ticker, value, and timestamp to avoid borrowing conflicts
        let data_to_process: Vec<(String, f64, i64)> = self
            .data
            .batch_points
            .iter()
            .filter_map(|p| {
                let ticker = p.tags.get(&self.options.ticker_field)?.clone();
                let value = p.fields_double.get(&self.options.field).cloned()?;
                Some((ticker, value, p.time)) // Include the timestamp
            })
            .collect();

        // Process the collected data
        for (ticker, value, timestamp) in data_to_process {
            let indicator_value = self.calculate_indicator(&ticker, value).await.unwrap();

            // Find the corresponding point and modify it
            if let Some(p) = self
                .data
                .batch_points
                .iter()
                .find(|p| p.tags.get(&self.options.ticker_field) == Some(&ticker))
            {
                let mut new_point = p.clone();
                new_point
                    .fields_double
                    .insert(self.options.as_field.clone(), indicator_value);
                new_point.time = timestamp; // Set the original timestamp

                // Send the updated point to Kapacitor
                debug!("Sending point: {:?}", new_point);
                self.send_response(Response {
                    message: Some(response::Message::Point(new_point)),
                })
                .await
                .map_err(|e| {
                    error!("Failed to send point response: {}", e);
                    io::Error::new(io::ErrorKind::Other, e)
                })?;
            }
        }

        // Send the EndBatch to Kapacitor
        debug!("Sending EndBatch");
        self.send_response(Response {
            message: Some(response::Message::End(end.clone())),
        })
        .await
        .map_err(|e| {
            error!("Failed to send EndBatch response: {}", e);
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn stop(&mut self) {
        debug!("Stop called, closing agent responses");
        let _ = self.responses.lock().await.close();
        debug!("IndicatorHandler stopped");
    }
}

impl std::fmt::Debug for IndicatorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndicatorHandler")
            .field("options", &self.options)
            .field("data_points_count", &self.data.batch_points.len())
            .finish()
    }
}
