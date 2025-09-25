String key = customer.getCustomerId() + "|" + account + "|" +
             (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
             (mobstatUrl != null ? mobstatUrl : "noMobstat") + "|" +
             (emailsForAccount.get("PDF") != null ? emailsForAccount.get("PDF") : "noEmailPdf") + "|" +
             (emailsForAccount.get("HTML") != null ? emailsForAccount.get("HTML") : "noEmailHtml") + "|" +
             (emailsForAccount.get("TEXT") != null ? emailsForAccount.get("TEXT") : "noEmailText");
