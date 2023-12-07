package alien.priority;

/**
 * @author Jørn-Are Flaten
 * @since 2023-12-04
 */
public class QueueProcessingDto {
    private int userId;
    private double cost;
    private long cputime;

    public QueueProcessingDto(int userId, long cost, long cputime) {
        this.userId = userId;
        this.cost = cost;
        this.cputime = cputime;
    }

    public QueueProcessingDto() {
    }

    public QueueProcessingDto(int userId) {
        this.userId = userId;
        this.cost = 0;
        this.cputime = 0;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public long getCputime() {
        return cputime;
    }

    public void setCputime(long cputime) {
        this.cputime = cputime;
    }

    public void addAccounting(double cost, long cputime) {
        this.cost += cost;
        this.cputime += cputime;
    }



}

