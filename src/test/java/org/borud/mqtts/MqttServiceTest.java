package org.borud.mqtts;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for MqttService.
 *
 * @author borud
 */
public class MqttServiceTest {
    @Test
    public void testBuilder() throws Exception {
        MqttService.newBuilder()
            .port(0)
            .connect((context, message) -> {return null;})
            .disconnect((context, message) -> {return null;})
            .publish((context, message) -> {return null;})
            .subscribe((context, message) -> {return null;})
            .unsubscribe((context, message) -> {return null;})
            .ping((context, message) -> {return null;})
            .build()
            .start()
            .shutdown();
    }
}

