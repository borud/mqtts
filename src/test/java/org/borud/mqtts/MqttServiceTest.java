package org.borud.mqtts;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.QoS;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for MqttService.
 *
 * @author borud
 */
public class MqttServiceTest {
    private static final Gson gson = new GsonBuilder()
        .setPrettyPrinting()
        .enableComplexMapKeySerialization()
        .serializeNulls()
        .create();

    @Test
    public void testBuilder() throws Exception {
        MqttService ms = MqttService.newBuilder()
            .port(0)
            .connect((context, message) -> {return null;})
            .disconnect((context, message) -> {return null;})
            .publish((context, message) -> {return null;})
            .subscribe((context, message) -> {return null;})
            .unsubscribe((context, message) -> {return null;})
            .ping((context, message) -> {return null;})
            .build();

        System.out.println(ms);

        ms.start().shutdown();
    }

    @Test
    public void testConnect() throws Exception {
        MqttService m = MqttService.newBuilder()
            .port(0)
            .connect((context, message) -> {
                    MqttFixedHeader header = new MqttFixedHeader(
                        MqttMessageType.CONNACK,
                        false,
                        message.fixedHeader().qosLevel(),
                        message.fixedHeader().isRetain(),
                        0
                    );

                    MqttConnAckVariableHeader body = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
                    MqttConnAckMessage response = new MqttConnAckMessage(header, body);

                    System.out.println("*** Message  ***\n" + gson.toJson(message));
                    System.out.println("*** Response ***\n" + gson.toJson(response));

                    return response;
                })

            .disconnect((context, message) -> {
                    return MqttService.DROP_CONNECTION;
                })

            .build();
        m.start();

        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", m.port());
        mqtt.setCleanSession(true);
        mqtt.setUserName("testuser");
        mqtt.setPassword("testpassword");
        mqtt.setConnectAttemptsMax(1);

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        assertTrue(connection.isConnected());

        // Publish a message
        // connection.publish("foo", "Hello".getBytes(), QoS.AT_LEAST_ONCE, false);

        connection.disconnect();
        m.shutdown();
    }
}

