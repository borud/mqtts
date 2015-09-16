# Random notes

Sketch 1

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

Sketch 2

    MqttService m = MqttService.newBuilder()
	    .login((context, username, password) -> {})
        .publish((context, message) -> {})
		.subscribe((context, subscribeRequest) -> {})
		.unsubscribe((context, unsubscribeRequest) -> {})
		.close((context) -> {})
		.build()
		.start((mqttService) -> {})
        .shutdown((mqttService) -> {})

    public interface Context {
            private String username;
		    private String password;
			private TopicManager topicManager;
    }

    public interface MqttService {
        public MqttService start();
        public MqttService shutdown();
		List<Clients> clients();
		Client client();
    }




    public interface Client {
        ListenableFuture<DeliveryResult>deliver(Message m);
    }


## Connect / Disconnect

Default handler which will do the connect/disconnect parts.
