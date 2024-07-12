package com.switchvov.magicmq.store;

import java.io.IOException;

/**
 * interface of file mapped
 *
 * @author switch
 * @since 2024/07/12
 */
public interface FileMapped {
    void init() throws IOException;

    void destroy();

    /**
     * write msg
     *
     * @param msg msg
     * @return the offset of msg
     */
    int write(byte[] msg);

    /**
     * read msg
     *
     * @param entry the entry of index of msg
     * @return msg data
     */
    byte[] read(Indexer.Entry entry);

    int getEndOffset();

    int getStorage();

    default void resetStartOffset(int offset) {

    }
}
