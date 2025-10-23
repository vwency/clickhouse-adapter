use clickhouse_orm::CHClient;

pub fn get_client() -> CHClient {
    CHClient::with_credentials("http://localhost:8123", "default", "default", "default")
}
