package alien.site;

public class SiteStatusDTO {

    private String siteId;
    private String statusId;
    private long count;
    private long totalCost;

    public SiteStatusDTO(String siteId, String statusId, long count, long totalCost) {
        this.siteId = siteId;
        this.statusId = statusId;
        this.count = count;
        this.totalCost = totalCost;
    }

    public SiteStatusDTO() {
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getStatusId() {
        return statusId;
    }

    public void setStatusId(String statusId) {
        this.statusId = statusId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(long totalCost) {
        this.totalCost = totalCost;
    }

    @Override
    public String toString() {
        return "SiteStatusDTO{" +
                "siteId='" + siteId + '\'' +
                ", statusId='" + statusId + '\'' +
                ", count=" + count +
                ", totalCost=" + totalCost +
                '}';
    }
}
