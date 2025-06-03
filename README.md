public class CustomerData {
    private String customerId;
    private String name;
    private String email;
    private String deliveryChannel;
    // add other fields as needed

    // Constructors
    public CustomerData() {}

    public CustomerData(String customerId, String name, String email, String deliveryChannel) {
        this.customerId = customerId;
        this.name = name;
        this.email = email;
        this.deliveryChannel = deliveryChannel;
    }

    // Getters and setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getDeliveryChannel() { return deliveryChannel; }
    public void setDeliveryChannel(String deliveryChannel) { this.deliveryChannel = deliveryChannel; }

    // Optionally override toString(), equals(), hashCode()
}
