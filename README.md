 int customersProcessed = 0;
            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String val = outputList.getAttribute("customersProcessed");
                if (val != null) {
                    try {
                        customersProcessed = Integer.parseInt(val);
                    } catch (NumberFormatException ignored) {}
                }
            }
