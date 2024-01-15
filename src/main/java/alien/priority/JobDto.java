package alien.priority;

public class JobDto {

    private int queueId;
    private int split;

    private int statusId;

    public JobDto(int queueId, int split, int statusId) {
        this.queueId = queueId;
        this.split = split;
        this.statusId = statusId;
    }

    public JobDto() {
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getSplit() {
        return split;
    }

    public void setSplit(int split) {
        this.split = split;
    }

    public int getStatusId() {
        return statusId;
    }

    public void setStatusId(int statusId) {
        this.statusId = statusId;
    }
}
