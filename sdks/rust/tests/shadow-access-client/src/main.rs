use shadow_access_client::test_handlers;

fn exit_on_panic() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_hook(panic_info);
        std::process::exit(1);
    }));
}

fn main() {
    env_logger::init();
    exit_on_panic();

    let db_name = std::env::var("SPACETIME_SDK_TEST_DB_NAME").expect("Failed to read db name from env");

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(test_handlers::run(&db_name));
}
