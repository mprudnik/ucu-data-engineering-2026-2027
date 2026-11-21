//! HTTP client side: [`core::Client`] over reqwest.

use crate::core;

/// Settings of a [`Client`].
#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL of the secondary, without a trailing `/`.
    pub url: String,
}

/// Sends requests to one secondary over HTTP.
///
/// Each request is a `POST` to `<url>/<METHOD>` with a JSON body.
pub struct Client {
    config: Config,
    http: reqwest::Client,
}

impl Client {
    /// Creates a client for the secondary at `config.url`.
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
            http: reqwest::Client::new(),
        }
    }
}

impl core::Client for Client {
    async fn send<R: core::Request>(&self, req: R) -> Result<R::Response, core::Error> {
        let url = format!("{}/{}", self.config.url, R::METHOD);

        let result = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await?
            .error_for_status()?;

        Ok(result.json().await?)
    }
}

impl From<reqwest::Error> for core::Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Transport {
            code: value.status().map_or(0, |s| s.as_u16() as i32),
            message: value.to_string(),
        }
    }
}
