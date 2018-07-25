package org.apache.flink.streaming.connectors.kudu.connect;

public class KuduOrder {

    private enum Direction {ASC, DESC}

    private String column;
    private Direction direction;

    private KuduOrder(String column, Direction direction) {
        this.column = column;
        this.direction = direction;
    }

    public String getColumn() {
        return column;
    }

    public Direction getDirection() {
        return direction;
    }

    public boolean ascending() {
        return Direction.ASC.equals(direction);
    }

    public boolean descending() {
        return Direction.DESC.equals(direction);
    }

    public static class Builder {
        private String column;
        private Direction direction;

        private Builder(String column) {
            this.column = column;
            this.direction = Direction.ASC;
        }

        public static Builder create(String column) {
            return new Builder(column);
        }

        public KuduOrder asc() {
            this.direction = Direction.ASC;
            return build();
        }

        public KuduOrder desc() {
            this.direction = Direction.DESC;
            return build();
        }

        public KuduOrder build() {
            return new KuduOrder(this.column, this.direction);
        }
    }
}
