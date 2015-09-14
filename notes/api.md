# Random notes

    MqttServer m = MqttServer.newBuilder()
        .connect((context, message) -> {})
        .disconnect((context, message) -> {})
        .publish((context, message) -> {})
        .subscribe((context, message) -> {})
        .unsubscribe((context, message) -> {})
        .ping((context, message) -> {})
        .port(9090)
        .build()
        .start(() -> {}
        .stop(() -> {});


    public interface Context {
        public String clientIdentifier();
        public Context clientIdentifier(String clientIdentifier);

        public String username();
        public Context username(String username);

    }
