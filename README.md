// Correctly escape double quotes for the JSON body
StringEntity entity = new StringEntity("{ \"password\": \"" + password + "\" }");
post.setEntity(entity);
