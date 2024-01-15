package alien.priority;

public class JobDto {

    private long queueId;
    private long split;

    private int statusId;

    public JobDto(long queueId, long split, int statusId) {
        this.queueId = queueId;
        this.split = split;
        this.statusId = statusId;
    }

    public JobDto() {
    }

    public long getQueueId() {
        return queueId;
    }

    public void setQueueId(long queueId) {
        this.queueId = queueId;
    }

    public long getSplit() {
        return split;
    }

    public void setSplit(long split) {
        this.split = split;
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
    }
}
