use crate::domain::client::client::CHClient;
use clickhouse::{Client, Compression};
use std::sync::Arc;

impl CHClient {
    pub fn new(url: impl Into<String>) -> Self {
        let client = Client::default().with_url(url.into());
        Self { inner: Arc::new(client) }
    }

    pub fn with_database(url: impl Into<String>, database: impl Into<String>) -> Self {
        let client = Client::default().with_url(url.into()).with_database(database.into());
        Self { inner: Arc::new(client) }
    }

    pub fn with_credentials(
        url: impl Into<String>,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let client = Client::default()
            .with_url(url.into())
            .with_database(database.into())
            .with_user(user.into())
            .with_password(password.into());
        Self { inner: Arc::new(client) }
    }

    pub fn builder() -> CHClientBuilder {
        CHClientBuilder::new()
    }

    pub(crate) fn client(&self) -> &Client {
        &self.inner
    }
}

pub struct CHClientBuilder {
    client: Client,
}

impl CHClientBuilder {
    pub fn new() -> Self {
        Self { client: Client::default() }
    }

    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.client = self.client.with_url(url.into());
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.client = self.client.with_database(database.into());
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.client = self.client.with_user(user.into());
        self
    }
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.client = self.client.with_password(password.into());
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.client = self.client.with_header(key.into(), value.into());
        self
    }

    pub fn option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.client = self.client.with_option(name.into(), value.into());
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.client = self.client.with_compression(compression);
        self
    }

    pub fn with_lz4_compression(self) -> Self {
        self.compression(Compression::Lz4)
    }

    pub fn with_no_compression(self) -> Self {
        self.compression(Compression::None)
    }

    pub fn build(self) -> CHClient {
        CHClient { inner: Arc::new(self.client) }
    }
}

impl Default for CHClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
