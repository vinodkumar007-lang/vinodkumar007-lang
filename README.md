public class CustomerSummary {

    private String accountNumber;
    private String cisNumber;
    private String customerId;
    private Map<String, String> deliveryStatus = new HashMap<>();
    private String status;

    public CustomerSummary() {}

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getCisNumber() {
        return cisNumber;
    }

    public void setCisNumber(String cisNumber) {
        this.cisNumber = cisNumber;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Map<String, String> getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(Map<String, String> deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "CustomerSummary{" +
                "accountNumber='" + accountNumber + '\'' +
                ", cisNumber='" + cisNumber + '\'' +
                ", customerId='" + customerId + '\'' +
                ", deliveryStatus=" + deliveryStatus +
                ", status='" + status + '\'' +
                '}';
    }
}
