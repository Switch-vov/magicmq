package com.switchvov.magicmq.store;

import com.switchvov.magicmq.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class StoreTest {

    private Store store;

    @BeforeEach
    void setUp() {
        store = new Store("magic-test");
        store.init();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void writeAndReader() {
        Message<String> m1 = Message.create("123", null);
        int pos = store.write(m1);
        Message<String> read = store.read(pos);
        assertEquals(m1, read);
        Message<String> m2 = Message.create("4567", null);
        pos = store.write(m2);
        read = store.read(pos);
        assertEquals(m2, read);
    }
}