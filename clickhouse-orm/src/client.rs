use clickhouse::Client;
use std::sync::Arc;

#[derive(Clone)]
pub struct CHClient {
    inner: Arc<Client>,
}

impl CHClient {
    pub fn new(url: impl Into<String>) -> Self {
        let client = Client::default().with_url(url.into());
        Self {
            inner: Arc::new(client),
        }
    }

    pub fn with_database(url: impl Into<String>, database: impl Into<String>) -> Self {
        let client = Client::default()
            .with_url(url.into())
            .with_database(database.into());
        Self {
            inner: Arc::new(client),
        }
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
        Self {
            inner: Arc::new(client),
        }
    }

    pub(crate) fn client(&self) -> &Client {
        &self.inner
    }
}
