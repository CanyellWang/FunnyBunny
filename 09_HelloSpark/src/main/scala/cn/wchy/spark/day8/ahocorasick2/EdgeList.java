package cn.wchy.spark.day8.ahocorasick2;

/**
 * Simple interface for mapping bytes to States.
 */
interface EdgeList {
    State get(byte ch);

    void put(byte ch, State state);

    byte[] keys();
}
