 // Add body with password
            StringEntity entity = new StringEntity("{ \"password\": \"\" + password + \"\" }");
            post.setEntity(entity);
