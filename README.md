// Created for: South Africa
// Created by : Esterhuizen, Jaco {CC616385}
// Created on : 2012.12.18
// Modified on: 2021.11.16

function FindProxyForURL(url, host)
{
    var ipaddr = /^\d+\.\d+\.\d+\.\d+$/g;
    var res_c_ip = URLhash(myIpAddress()) % 3;
    var res_s_ip = URLhash(host) % 3;
    
    var proxy_0 = "PROXY 172.17.2.12:80; PROXY 172.17.2.243:80; PROXY 172.17.2.244:80";
    var proxy_1 = "PROXY 172.17.2.243:80; PROXY 172.17.2.244:80; PROXY 172.17.2.12:80";
    var proxy_2 = "PROXY 172.17.2.244:80; PROXY 172.17.2.12:80; PROXY 172.17.2.243:80";
    var proxy_5 = "PROXY 172.25.16.11:80; PROXY 172.25.16.12:80";
    var proxy_6 = "PROXY 172.25.16.12:80; PROXY 172.25.16.11:80";

    if (dnsDomainIs(host, "frwl.nedsecure.nednet.co.za") ||
       	(host == "feedbk.nedsecure.nednet.co.za") ||
       	(host == "img.nedsecure.nednet.co.za") ||
       	(host == "soc.nedsecure.nednet.co.za") ||
       	(host == "stats.nedsecure.nednet.co.za") ||
       	(host == "feedbk.nednet.co.za") ||
       	(host == "soc.nednet.co.za") ||
       	(host == "img.nednet.co.za") ||
       	(host == "stats.nednet.co.za") ||
       (host == "frwl.nedsecure.nednet.co.za"))
       {
        return "PROXY 172.17.2.9:80; PROXY 172.17.2.10:80; PROXY 172.17.2.11:80";
       }   

//FORCE TRAFFIC FOR IDENTITYNOW TO BREDELL PROD - START
    if ((host == "nedbank.identitynow.com") ||
        (host == "nedbank-sb.identitynow.com"))  
    {
	  return "PROXY 10.59.10.31:80";
	}
//FORCE TRAFFIC FOR IDENTITYNOW TO BREDELL PROD - END

	if (isPlainHostName(host) ||
           dnsDomainIs(host, ".nednet.co.za") ||
        (host == "d365web.nedcloud.co.za") ||
        (host == "frontier.payshap.bankservafrica.cloud") ||
	(host == "psiberswzqa01.nedswazi.net") ||
	(host == "psiberswzpr01.nedswazi.net") ||
	(host == "qa-psiberwz.nedbank.co.sz") ||
	(host == "psiberswz.nedbank.co.sz") ||
	(host == "psiberzimqa01.mbca.co.zw") ||
	(host == "psiberzimpr01.mbca.co.zw") ||
	(host == "psiberzim.nedbank.co.zw") ||
	(host == "qa-psiberzim.nedbank.co.zw") ||
	    (host == "168.142.249.97") ||
	    (host == "168.142.249.98") ||
	    (host == "168.142.249.113") ||
	    (host == "168.142.249.114") ||
	    (host == "rtgs.rbz.co.zw") ||
	(host == "psibernamqa1.namibia.cbon.root") ||
	(host == "psibernampr01.namibia.cbon.root") ||
	(host == "psibernam.namibia.cbon.root") ||
	(host == "qa-psibernam.namibia.cbon.root") ||
	(host == "psiberlespr01.nedcor.com") ||
	(host == "psiberlesqa01.nedcor.com") ||
	(host == "psiberles.nedbank.co.ls") ||
	(host == "qa-psiberles.nedbank.co.ls") ||
	(host == "qa-psiberswz.nedbank.co.sz") ||
	(shExpMatch(host, "securitease-nedbank.iress.co.za")) ||
	(shExpMatch(host, "securitease-nedbanke2.iress.co.za")) ||
	(shExpMatch(host, "securitease-nedgroup.iress.co.za")) ||
	(shExpMatch(host, "securitease-nedgrpe2.iress.co.za")) ||
	(shExpMatch(host, "securitease-nedbankcts.iress.co.za")) ||
	(shExpMatch(host, "securitease-nedgrpcts.iress.co.za")) ||
	(shExpMatch(host, "tfm-training.bankservafrica.com")) ||
	(shExpMatch(host, "tfm-uat.bankservafrica.com")) ||
	(shExpMatch(host, "tfm-production.bankservafrica.com")) ||
	(shExpMatch(host, "nsnprodsbcmovoc1.nedbank.co.za")) ||
	(shExpMatch(host, "nsnprodsbcmovoc2.nedbank.co.za")) ||
	(shExpMatch(host, "nsn-prod-rgp-sbc-001-mgr.nedbank.co.za")) ||
            (shExpMatch(host, "nsnqaasaesg01.privatelink.dev.azuresynapse.net")) ||
            (shExpMatch(host, "nsnqaasaesg01.dev.azuresynapse.net")) ||
            (shExpMatch(host, "nsnqaasaesg01-ondemand.sql.azuresynapse.net")) ||
            (shExpMatch(host, "nsnqaasaesg01-ondemand.privatelink.sql.azuresynapse.net")) ||
            (shExpMatch(host, "nsnqaasaesg01.privatelink.sql.azuresynapse.net")) ||
            (shExpMatch(host, "nsnqaasaesg01.sql.azuresynapse.net")) ||
           dnsDomainIs(host, ".nedcor.net") ||
           dnsDomainIs(host, ".NEDCOR.NET") ||
           dnsDomainIs(host, ".it.nednet.co.za") ||
           dnsDomainIs(host, ".africa.nedcor.net") ||
           dnsDomainIs(host, ".nedcorqa.nedcor.net") ||
           dnsDomainIs(host, ".nedete.net") ||
           dnsDomainIs(host, ".boe.co.za") ||
           dnsDomainIs(host, ".namibia.cbon.root") ||
           dnsDomainIs(host, ".nedawsdev.net") ||
           dnsDomainIs(host, ".nedawsete.net") ||
           dnsDomainIs(host, ".nedawsqa.net") ||
           dnsDomainIs(host, ".nedaws.net") ||
        dnsDomainIs(host, ".it.nedcloud.co.za") ||
        dnsDomainIs(host, ".int.nedcloud.co.za") ||
           (host == "pl-portal-sandbox-dev-apim.nedcloud.co.za") ||
           (host == "pl-portal-live-dev-apim.nedcloud.co.za") ||
           (host == "fids-portal-sandbox-dev-apim.nedcloud.co.za") ||
           (host == "fids-portal-live-dev-apim.nedcloud.co.za") ||
           (host == "auth-portal-sandbox-dev-apim.nedcloud.co.za") ||
           (host == "auth-portal-live-dev-apim.nedcloud.co.za") ||
           (host == "nar-auth-portal-sandbox-dev-apim.nedcloud.co.za") ||
           (host == "nar-auth-portal-live-dev-apim.nedcloud.co.za") ||
           (host == "nsnetesignalrteller.service.signalr.net") ||
           (host == "nsnqasignalrteller.service.signalr.net") ||
           (host == "nsnsignalrteller.service.signalr.net") ||
           (host == "nsndevsignalrteller.service.signalr.net") ||
           (host == "nsndevsignalrd365001.privatelink.service.signalr.net") ||
           (host == "nsnetesignalrd365001.privatelink.service.signalr.net") ||
           (host == "nsnqasignalrd365001.privatelink.service.signalr.net") ||
           (host == "nsnsignalrd365001.privatelink.service.signalr.net") ||
           (host == "southafricanorth.pl-auth.privatelink.azuredatabricks.net") ||
           (host == "adb-4342428048965081.1.privatelink.azuredatabricks.net") ||
           (host == "adb-4342428048965081.1.azuredatabricks.net") ||
           (host == "southafricanorth.pl-auth.azuredatabricks.net") ||
           (host == "adb-2416944836898798.18.azuredatabricks.net") ||
        (host == "www.internetbanking.nedbank.com.na") ||
        (host == "105.29.157.137") ||
        (host == "qastats.mfcauctions.co.za") ||
        (host == "qa360.mfcauctions.co.za") ||
        (host == "qawww.mfcauctions.co.za") ||
        (host == "qalive.mfcauctions.co.za") ||
        (host == "qacypress.mfcauctions.co.za") ||
        (host == "qaac.mfcauctions.co.za") ||
        (host == "ete-d365web.nedcloud.co.za") ||
        (host == "qa-d365web.nedcloud.co.za") ||
        (host == "prod-d365web.nedcloud.co.za") ||
           dnsDomainIs(host, ".nedazhybcld.co.za") ||
           dnsDomainIs(host, ".nedawshybcld.co.za") ||
           dnsDomainIs(host, ".nedophybcld.co.za") ||
           dnsDomainIs(host, ".nedcldintete.net") ||
           dnsDomainIs(host, ".nedcldextete.net") ||
           dnsDomainIs(host, ".nedcldintqa.net") ||
           dnsDomainIs(host, ".nedcldextqa.net") ||
           dnsDomainIs(host, ".nedcldextprod.net") ||
           dnsDomainIs(host, ".nedcldintprod.net") ||
           dnsDomainIs(host, ".nedazure.net") ||
           dnsDomainIs(host, ".nedazureqa.net") ||
           dnsDomainIs(host, ".nedazureete.net") ||
           dnsDomainIs(host, ".corporatesaver.co.za")||
           dnsDomainIs(host, ".nedbank-office.co.za") ||
           dnsDomainIs(host, ".autodiscover.nedbank.co.za") ||
           dnsDomainIs(host, ".silica.net") ||
           dnsDomainIs(host, ".session.rservices.com") ||
           dnsDomainIs(host, ".ezcomm.co.za") ||
           dnsDomainIs(host, "titan.nedbank.co.za") ||
           dnsDomainIs(host, "titanreports.nedbank.co.za") ||
           dnsDomainIs(host, "nfpsp.nfpqa.co.za") ||
           dnsDomainIs(host, "nfpsp.nedbank.co.za") ||
           dnsDomainIs(host, ".titan.nedbank.co.za") ||
           dnsDomainIs(host, "devbireports.nfpqa.co.za") ||
           dnsDomainIs(host, "qabireports.nfpqa.co.za") ||
           dnsDomainIs(host, "bireports.nedbank.co.za") ||
           dnsDomainIs(host, "code.nfpqa.co.za") ||
           dnsDomainIs(host, ".nedcm.co.za") ||
           dnsDomainIs(host, ".redi-online.co.za") ||
           dnsDomainIs(host, ".nedqrmqa.net") ||
           dnsDomainIs(host, ".br34.websure.co.za") ||
           dnsDomainIs(host, ".nedsec.co.za") ||
           dnsDomainIs(host, ".ombs.omplc.net") ||
           dnsDomainIs(host, ".apps.omplc.net") ||
           dnsDomainIs(host, ".uatpayir.resbank.co.za") ||
           dnsDomainIs(host, ".imperial-bank.org") ||
           dnsDomainIs(host, ".intrazone.co.za") ||
           dnsDomainIs(host, ".test-ned.net") ||
           dnsDomainIs(host, ".travellinck.com") ||
           dnsDomainIs(host, ".bancounico.local") ||
           dnsDomainIs(host, ".accesswca.com") ||
           dnsDomainIs(host, ".nedswazi.net") ||
           dnsDomainIs(host, ".nedmalawi.net") ||
           dnsDomainIs(host, ".nedcor.com") ||
           dnsDomainIs(host, ".mbca.co.zw") ||
           dnsDomainIs(host, ".prd.nedtrade.net") ||
           dnsDomainIs(host, ".dr.nedtrade.net") ||
           dnsDomainIs(host, ".qa.nedtrade.net") ||
        dnsDomainIs(host, ".thomsonreuters.biz") ||
        dnsDomainIs(host, ".thomsonreuters.net") ||
        (host == "159.43.16.165") ||
        (host == "159.43.16.166") ||
        (host == "159.43.16.167") ||
        (host == "159.43.22.21") ||
        (host == "159.43.22.22") ||
        (host == "159.43.22.23") ||
        dnsDomainIs(host, ".refinitiv.biz") ||
        dnsDomainIs(host, ".refinitiv.net") ||
	dnsDomainIs(host, ".appserviceenvironment.net") ||
           shExpMatch(url, "*//genius.nednet.co.za/*") ||
           shExpMatch(url, "*//qa-genius.nednet.co.za/*") ||
           shExpMatch(url, "*//cura.it.nednet.co.za/*") ||
           shExpMatch(url, "*//nedbank1.rqtech.co.za/*") ||
           shExpMatch(url, "*//nedbank2.rqtech.co.za/*") ||
           shExpMatch(url, "*//nedbank3.rqtech.co.za/*") ||
		(host == "www.iuqerfsodp9ifjaposdfjhgosurijfaewrwergwea.com") ||
 	dnsDomainIs(host, "creditnowdirect.experian.co.za") ||
      	   (host == "creditnowdirect.experian.co.za") ||
      	   (host == "ete-neddepositnotes.nedbank.co.za") ||
      	   (host == "ete-nedinvestmenttrust.nedbank.co.za") ||
      	   (host == "mw.ext1.markit.com") ||
           (host == "mw.ext2.markit.com") ||
           (host == "mw.ext3.markit.com") || 
       	   (host == "mw.api-ext1.markit.com") ||
           (host == "mw.api-ext2.markit.com") ||
           (host == "mw.api-ext3.markit.com") ||
	(host == "q-nrum.nedbank.co.za") ||
	(host == "nrum.nedbank.co.za") ||
           (host == "196.15.73.95") ||
           (host == "209.234.233.231") ||
           (host == "209.234.233.232") ||
           (host == "209.234.233.233") ||
           (host == "209.234.233.234") ||
           (host == "209.234.233.235") ||
           (host == "209.234.233.236") ||
           (host == "148.173.177.213") ||
           (host == "168.142.248.39") ||
           (host == "196.4.7.29") ||
           (host == "196.15.168.21") ||
           (host == "196.15.73.65") ||
           (host == "196.30.132.142") ||
           (host == "196.34.233.242") ||
           (host == "196.34.233.248") ||
           (host == "196.34.233.250") ||
           (host == "196.216.153.200") ||
           (host == "196.216.153.202") ||
           (host == "196.36.206.80") ||
           (host == "196.38.254.11") ||
           (host == "165.233.50.213") ||
           (host == "165.233.50.214") ||
           (host == "165.233.50.215") ||
           (host == "165.233.50.216") ||
           (host == "196.15.237.162") ||
           (host == "196.15.237.161") ||
           (host == "196.34.233.250") ||
           (host == "196.35.209.233") ||
           (host == "196.35.209.234") ||
           (host == "196.38.188.131") ||
           (host == "196.38.209.79") ||
	   (host == "91.240.72.33") ||
	(host == "213.86.119.241") ||
	(host == "qa-flexadmin.nedswazi.net") ||
	(host == "flexadmin.nedswazi.net") ||
        (host == "wpvwe99a0014.cnet.trzn.wachovia.net") ||
        (host == "wpvwe98a0014.cnet.trzn.wachovia.net") ||
        (host == "wrvwe99a0014.cnet.trzn.wachovia.net") ||
        (host == "wrvwe98a0014.cnet.trzn.wachovia.net") ||
        (host == "wuvwe99a0014.ctch.xtrt.wachovia.net") ||
        (host == "eximbills.wellsfargo.com") ||
        (host == "eximbills-prod1.wellsfargo.com") ||
        (host == "eximbills-prod2.wellsfargo.com") ||
        (host == "eximbills-dr.wellsfargo.com") ||
        (host == "eximbills-dr1.wellsfargo.com") ||
        (host == "eximbills-dr2.wellsfargo.com") ||
        (host == "eximbills-uat.wellsfargo.com") ||
        (host == "eximbills-uat1.wellsfargo.com") ||
        (host == "uat.ia.swapswire.com") ||
        (host == "cai.maitlandgroup.com") ||
        (host == "qa-fcdbadmin.nedcor.com") ||
        (host == "sso-qa.nedcor.com") ||
        (host == "qa-fcubsadmin.nedcor.com") ||
        (host == "qa-flexadmin.nedcor.com") ||
        (host == "flexadmin.nedcor.com") ||
           (host == "viewsec.silica.net") ||
           (host == "eximbills.wellsfargo.com") ||
           (host == "nfp-main-authmanager.nfp.local") ||
           (host == "mail.nedbank.co.za") ||
           (host == "autodiscover.nedbank.co.za") ||
           (host == "fix.accesswca.com") ||
           (host == "fsfix.accesswca.com") ||
           (host == "fs.accesswca.com") ||
           (host == "wellscontent.wellsfargo.com") ||
           (host == "pltxuat.lcmatrix.com") ||
           (host == "pltxfix.lcmatrix.com") ||
           (host == "pltxbcp.lcmatrix.com") ||
           (host == "pltx.lcmatrix.com") ||
           (host == "bugnet.nedbank.co.za") ||
           (host == "central199.intra.aexp.com") ||
           (host == "central643.intra.aexp.com") ||
           (host == "central644.intra.aexp.com") ||
           (host == "central645.intra.aexp.com") ||
           (host == "central646.intra.aexp.com") ||
           (host == "connect.nedbank.co.za") ||
           (host == "dealerwebqa.mfc.co.za") ||
           (host == "global.lcmatrix.com") ||
           (host == "global.cte.lcmatrix.com") ||
           (host == "online-qa.nedgrouplife.co.za") ||
           (host == "online.nedgrouplife.co.za") ||
           (host == "hunter.experian.co.za") ||
           (host == "huntertest.experian.co.za") ||
           (host == "ionline.nedgrouplife.co.za") ||
           (host == "ionlineqa.nedgrouplife.co.za") ||
           (host == "nedbankmpls.tradertools.com") ||
        (host == "kars.tch.sa-etoll.co.za") ||
           (host == "ka.tch.sa-etoll.co.za") ||
           (host == "ka-dr.tch.sa-etoll.co.za") ||
           (host == "katest.tch.sa-etoll.co.za") ||
           (host == "leadtracker.nfpqa.co.za") ||
           (host == "dialin.nedbank.co.za") ||
           (host == "meet.africa.nedcor.net") ||
           (host == "meet.boe.co.za") ||
           (host == "meet.imperialbank.co.za") ||
           (host == "meet.mfc.co.za") ||
           (host == "meet.nedbank.co.za") ||
           (host == "meet.nedbank.com") ||
           (host == "meet.nedcor.net") ||
           (host == "meet.nedbankcapital.co.za") ||
           (host == "meet.nedgroupinvestments.co.za") ||
           (host == "meet.nedgrouplife.co.za") ||
           (host == "meet.nedlife.co.za") ||
           (host == "meet.nedbankprivatewealth.co.za") ||
           (host == "meet.nedbank.co.uk") ||
           (host == "meet.nedbankcapital.co.uk") ||
           (host == "meet.nedbank.co.ke") ||
           (host == "meet.nedbankinsurance.co.za") ||
           (host == "mercury.dimensiondata.com") ||
           (host == "mssdv.jse.co.za") ||
           (host == "msstrain.jse.co.za") ||
           (host == "private.approver.co.za") ||
           (host == "qcentral607.intra.aexp.com") ||
           (host == "qcentral606.intra.aexp.com") ||
           (host == "qamobile.nedbank-office.co.za") ||
           (host == "qawebaccess.nedbank.co.za") ||
           (host == "secure.nedbank.co.za") ||
           (host == "secure.nedgrouplife.co.za") ||
           (host == "secure-qa.nedgrouplife.co.za") ||
           (host == "securid.mercury.co.za") ||
           (host == "securemail.nedbank.co.za") ||
           (host == "services.dimensiondata.com") ||
           (host == "titan.nedbank.co.za") ||
           (host == "titan.nfpqa.co.za") ||
           (host == "idu.nedbank.co.za") ||
           (host == "leadtracker.nedbank.co.za") ||
           (host == "Nfpmobi.nedbank.co.za") ||
           (host == "nfpbus.nedbank.co.za") ||
           (host == "titanreports.nedbank.co.za") ||
           (host == "titanreports.nfpqa.co.za") ||
           (host == "infinitynedbank.xplan.iress.co.za") ||
           (host == "nfppreprod.xplan.iress.co.za") ||
           (host == "xplannfp.xplan.iress.co.za") ||
           (host == "nfptraining.xplan.iress.co.za") ||
           (host == "voltage-pp-0000.nedbank.co.za") ||
           (host == "webaccess.nedbank.co.za") ||
           (host == "sip.boe.co.za") ||
           (host == "webconf.nedbank.co.za") ||
           (host == "sip.nedbank.co.za") ||
           (host == "av.nedbank.co.za") ||
           (host == "mfc.inivit.com") ||
           (host == "mfcstaging.inivit.com") ||
           (host == "ac.inivit.com") ||
           (host == "stgac.inivit.com") || 
           (host == "sip.nedgroupinvestments.co.za") ||
           (host == "sip.nedgrouplife.co.za") ||
           (host == "sip.mfc.co.za") ||
           (host == "sip.nedbank.com") ||
           (host == "access.nedbank.co.za") ||
           (host == "sip.nedbankcapital.co.za") ||
           (host == "sip.nedlife.co.za") ||
           (host == "lyncdiscover.nedbank.co.za") ||
           (host == "lyncdiscover.boe.co.za") ||
           (host == "lyncdiscoverinternal.nedbank.co.za") ||
           (host == "aprimo.nfpqa.co.za") ||
           (host == "idu.nfpqa.co.za") ||
           (host == "www.bugnet.nfpqa.co.za") ||
           (host == "nfpbus.nfpqa.co.za") ||
           (host == "uatpayir.resbank.co.za") ||
           (host == "dial.nedbank.co.za") ||
           (host == "qa-fcdbadmin.nedswazi.net") ||
           (host == "qa-fcdbonboarding.nedswazi.net") ||
           (host == "sso-qa.nedswazi.net") ||
           (host == "fcdbonboarding.nedswazi.net") ||
           (host == "fcdbadmin.nedswazi.net") ||
	   (host == "qa-flexadmin.nedmalawi.net") ||
	   (host == "flexadmin.nedmalawi.net") ||
	   (host == "sso-qa.nedmalawi.net") ||
	   (host == "sso-qa.mbca.co.zw") ||
	   (host == "qa-flexadmin.mbca.co.zw") ||
	   (host == "flexadmin.mbca.co.zw") ||
        (host == "160.123.18.5") ||
        (host == "160.123.18.6") ||
        (host == "160.123.18.7") ||
        (host == "160.123.18.8") ||
        (host == "160.123.18.9") ||
        (host == "hfm-qa.om.oldmutual.com") ||
        (host == "hfm-prod.om.oldmutual.com") ||
        (host == "dctuat.app.omem.net") ||
        (host == "spscheduleruat.app.omem.net") ||
        (host == "dctpp.app.omem.net") ||
        (host == "spschedulerpp.app.omem.net") ||
        (host == "dct.app.omem.net") ||
        (host == "spscheduler.app.omem.net") ||
        (host == "sts.nedsecure.co.za") ||
        (host == ".accesswca.com") ||
        (host == "196.34.233.12") ||
          (host == "net-beta.iress.co.za") ||
          (host == "196.6.238.26") ||
          (host == "connect.nedbank.co.ls") ||
          (host == "autodiscover.nedbank.co.ls") ||
          (host == "legacy.nedbank.co.ls") ||
          (host == "connect.nedbank.co.mw") ||
          (host == "autodiscover.nedbank.co.mw") ||
          (host == "legacy.nedbank.co.mw") ||
          (host == "connect.nedbank.co.zw") ||
          (host == "autodiscover.nedbank.co.zw") ||
          (host == "legacy.nedbank.co.zw") ||
          (host == "securesignir.com") ||
           dnsDomainIs(host, "securesignir.com") ||
           (host == "otp-secure.nedbank.co.za") ||
           (host == "token-secure.nedbank.co.za") ||
           (host == "mfa-secure.nedbank.co.za") ||
           (host == "secureconnect.nedbank.co.za") ||
           (host == "secureconnect2.nedbank.co.za") ||
           (host == "secureconnect3.nedbank.co.za") ||
           (host == "secureconnect4.nedbank.co.za") ||
        (host == "businessbanking.nedsecure.co.za") ||
           (host == "servicenowqa.nedcloud.co.za") ||
           (host == "servicenowdev.nedcloud.co.za") ||
           (host == "servicenow.nedcloud.co.za") ||
           (host == "nsnqaunlwb001.nedcloud.co.za") ||
           (host == "nsnqaunlwb002.nedcloud.co.za") ||
           (host == "nsnqaunlimage.nedcloud.co.za") ||
           (host == "nsnprunlwb001.nedcloud.co.za") ||
           (host == "nsnprunlwb002.nedcloud.co.za") ||
           (host == "nsnprunlimage.nedcloud.co.za") ||
           (host == "nsnprsimpwb002.nedcloud.co.za") ||
           (host == "nsnprsimpimage.nedcloud.co.za") ||
           (host == "internal.unlocked.me") ||
           (host == "emea5fms1.adobeconnect.com") ||
           (host == "emea5fms2.adobeconnect.com") ||
           (host == "emea5fms3.adobeconnect.com") ||
           (host == "emea5fms4.adobeconnect.com") ||
           (host == "emea5fms5.adobeconnect.com") ||
           (host == "emea5fms6.adobeconnect.com") ||
           (host == "emea5fms7.adobeconnect.com") ||
           (host == "emea5fms8.adobeconnect.com") ||
           (host == "emea5fms9.adobeconnect.com") ||
           (host == "emea5fms10.adobeconnect.com") ||
           (host == "emea5fms11.adobeconnect.com") ||
           (host == "emea5fms12.adobeconnect.com") ||
           (host == "emea5fms13.adobeconnect.com") ||
           (host == "emea5fms14.adobeconnect.com") ||
           (host == "emea5fms15.adobeconnect.com") ||
           (host == "internal.simplybiz.co.za") ||
           (host == "nsnqasimpint001.nedcloud.co.za") ||
           (host == "support.simplybiz.co.za") ||
           (host == "net-ct.iress.co.za") ||
           (host == "196.35.14.167") ||
           (host == "nsndvunlwb001.nedcloud.co.za") ||
           (host == "nsndvunlwb002.nedcloud.co.za") ||
           (host == "nsnqasimpwb002.nedcloud.co.za") ||
           (host == "nsndvsimpwb001.nedcloud.co.za") ||
           (host == "nsndvsimpwb001test.nedcloud.co.za") ||
        (host == "lyncportal.nedbank.co.za"))
        {return "DIRECT";}
	
// FORCE THESE TeamsLive TRAFFIC DIRECT - START
if (shExpMatch(host, "*.streaming.mediaservices.windows.net") ||
    shExpMatch(host, "*.azureedge.net") ||
    shExpMatch(host, "*.bmc.cdn.office.net") ||
    shExpMatch(host, "*.media.azure.net"))
    {
     var resolved_ip = dnsResolve(host);
      if (isInNet(resolved_ip, '152.199.19.161', '255.255.255.255') ||
          isInNet(resolved_ip, '68.232.34.200', '255.255.255.255') ||
          isInNet(resolved_ip, '152.195.19.97', '255.255.255.255') ||
          isInNet(resolved_ip, '152.199.21.175', '255.255.255.255'))
          {return "DIRECT";}
    }
// FORCE THESE TeamsLive TRAFFIC DIRECT - END

//DIRECT TEAMS TRAFFIC DIRECT WHILE ON VPN - START
if ((shExpMatch(host, "*.teams.microsoft.com"))
	|| (shExpMatch(host, "remote.nedbank.co.za"))
	|| (shExpMatch(host, "q-remote.nedbank.co.za"))
        || (shExpMatch(host, "teams.cloud.microsoft"))
        || (shExpMatch(host, "*.teams.cloud.microsoft"))
   	|| (shExpMatch(host, "teams.microsoft.com")))
	{
	if (isInNet(myIpAddress(), '172.28.0.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.4.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.8.0', '255.255.252.0')||
   	    isInNet(myIpAddress(), '172.28.16.0', '255.255.254.0')||
   	    isInNet(myIpAddress(), '172.28.18.0', '255.255.254.0')||
    	    isInNet(myIpAddress(), '172.28.32.0', '255.255.224.0')||
    	    isInNet(myIpAddress(), '172.28.64.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.68.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.72.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.76.0', '255.255.252.0')||
            isInNet(myIpAddress(), '172.28.254.0', '255.255.255.0')||
            isInNet(myIpAddress(), '172.28.160.0', '255.255.254.0')||
            isInNet(myIpAddress(), '172.28.180.0', '255.255.252.0')||
    	    isInNet(myIpAddress(), '172.28.202.0', '255.255.254.0')||
   	    isInNet(myIpAddress(), '172.28.253.0', '255.255.255.0'))
	{return "DIRECT";}}
//DIRECT TEAMS TRAFFIC DIRECT WHILE ON VPN - END

     if ((shExpMatch(host, "*.office365.com"))
	|| (shExpMatch(host, "clientconfig.microsoftonline-p.net"))
	|| (shExpMatch(host, "*.powerautomate.us"))
	|| (shExpMatch(host, "*.appsplatform.us"))
        || (shExpMatch(host, "*.microsoft365.com"))
	|| (shExpMatch(host, "*.powerautomate.com"))
        || (shExpMatch(host, "platform.bing.com"))
        || (shExpMatch(host, "chromedriver.storage.googleapis.com"))
        || (shExpMatch(host, "arc.msn.com"))
        || (shExpMatch(host, "bing.com"))
        || (shExpMatch(host, "*.s-microsoft.com"))
        || (shExpMatch(host, "*.microsoftcloud.com"))
        || (shExpMatch(host, "*.directline.botframework.com"))
        || (shExpMatch(host, "*.powerplatform.com"))
        || (shExpMatch(host, "catalog.meshxp.net"))
        || (shExpMatch(host, "avatars.meshxp.net"))
        || (shExpMatch(host, "*.microsoft"))
        || (shExpMatch(host, "onenote.com"))
        || (shExpMatch(host, "*.powerplatformusercontent.com"))
	|| (shExpMatch(host, "osiprod-cus-daffodil-signalr-00.service.signalr.net"))
	|| (shExpMatch(host, "osiprod-neu-daffodil-signalr-00.service.signalr.net"))
	|| (shExpMatch(host, "osiprod-weu-daffodil-signalr-00.service.signalr.net"))
	|| (shExpMatch(host, "osiprod-wus-daffodil-signalr-00.service.signalr.net"))
	|| (shExpMatch(host, "informationprotection.hosting.portal.azure.net"))
	|| (shExpMatch(host, "*.virtualearth.net"))
	|| (shExpMatch(host, "*.microsoftusercontent.com"))
	|| (shExpMatch(host, "*.hip.live.com"))
	|| (shExpMatch(host, "cdn.uci.officeapps.live.com"))
	|| (shExpMatch(host, "*.appex-rf.msn.com"))
	|| (shExpMatch(host, "*.cortana.ai"))
	|| (shExpMatch(host, "activity.windows.com"))
	|| (shExpMatch(host, "*.dynamics.com"))
	|| (shExpMatch(host, "*.symcb.com"))
	|| (shExpMatch(host, "*broadcast.officeapps.live.com"))
	|| (shExpMatch(host, "*cdn.onenote.net"))
	|| (shExpMatch(host, "*excel.officeapps.live.com"))
	|| (shExpMatch(host, "*onenote.officeapps.live.com"))
	|| (shExpMatch(host, "*powerpoint.officeapps.live.com"))
	|| (shExpMatch(host, "*view.officeapps.live.com"))
	|| (shExpMatch(host, "*visio.officeapps.live.com"))
	|| (shExpMatch(host, "*word-edit.officeapps.live.com"))
	|| (shExpMatch(host, "*word-view.officeapps.live.com"))
	|| (shExpMatch(host, "*.aadconnecthealth.azure.com"))
	|| (shExpMatch(host, "*.aadrm.com"))
	|| (shExpMatch(host, "*.acompli.net"))
	|| (shExpMatch(host, "*.adhybridhealth.azure.com"))
	|| (shExpMatch(host, "*.adl.windows.com"))
	|| (shExpMatch(host, "*.appex.bing.com"))
	|| (shExpMatch(host, "*.appex-rf.msn.com"))
	|| (shExpMatch(host, "*.assets-yammer.com"))
	|| (shExpMatch(host, "*.azure-apim.net"))
	|| (shExpMatch(host, "*.azureedge.net"))
	|| (shExpMatch(host, "*.azurerms.com"))
	|| (shExpMatch(host, "*.cdn.optimizely.com"))
	|| (shExpMatch(host, "*.cloudapp.net"))
	|| (shExpMatch(host, "*.cloudfront.net"))
	|| (shExpMatch(host, "*.dc.trouter.io"))
	|| (shExpMatch(host, "*.digicert.com"))
	|| (shExpMatch(host, "*.entrust.com"))
	|| (shExpMatch(host, "*.entrust.net"))
	|| (shExpMatch(host, "*.ext.azure.com"))
	|| (shExpMatch(host, "*.geotrust.com"))
	|| (shExpMatch(host, "*.giphy.com"))
	|| (shExpMatch(host, "*.helpshift.com"))
	|| (shExpMatch(host, "*.hockeyapp.net"))
	|| (shExpMatch(host, "*.hybridconfiguration.azurewebsites.net"))
	|| (shExpMatch(host, "*.localytics.com"))
	|| (shExpMatch(host, "*.log.optimizely.com"))
	|| (shExpMatch(host, "*.lync.com"))
	|| (shExpMatch(host, "*.microsoft.com"))
	|| (shExpMatch(host, "*.microsoftonline.com"))
	|| (shExpMatch(host, "*.microsoftonline-p.com"))
	|| (shExpMatch(host, "*.msappproxy.net"))
	|| (shExpMatch(host, "*.msecnd.net"))
	|| (shExpMatch(host, "*.msedge.net"))
	|| (shExpMatch(host, "*.msocdn.com"))
	|| (shExpMatch(host, "*.mstea.ms"))
	|| (shExpMatch(host, "*.o365weve.com"))
	|| (shExpMatch(host, "*.office.com"))
	|| (shExpMatch(host, "*.office.net"))
	|| (shExpMatch(host, "*.officeapps.live.com"))
	|| (shExpMatch(host, "*.omniroot.com"))
	|| (shExpMatch(host, "*.onedrive.com"))
	|| (shExpMatch(host, "*.onenote.com"))
	|| (shExpMatch(host, "*.onenote.net"))
	|| (shExpMatch(host, "*.onmicrosoft.com"))
	|| (shExpMatch(host, "*.outlook.com"))
	|| (shExpMatch(host, "*.passport.net"))
	|| (shExpMatch(host, "*.phonefactor.net"))
	|| (shExpMatch(host, "*.portal.azure.com"))
	|| (shExpMatch(host, "*.portal.cloudappsecurity.com"))
	|| (shExpMatch(host, "*.powerapps.com"))
	|| (shExpMatch(host, "*.powerbi.com"))
	|| (shExpMatch(host, "*.public-trust.com"))
	|| (shExpMatch(host, "*.search.production.apac.trafficmanager.net"))
	|| (shExpMatch(host, "*.search.production.emea.trafficmanager.net"))
	|| (shExpMatch(host, "*.search.production.us.trafficmanager.net"))
	|| (shExpMatch(host, "*.secure.aadcdn.microsoftonline-p.com"))
	|| (shExpMatch(host, "*.secure.skypeassets.com"))
	|| (shExpMatch(host, "*.sfbassets.com"))
	|| (shExpMatch(host, "*.sharepoint.com"))
	|| (shExpMatch(host, "*.sharepointonline.com"))
	|| (shExpMatch(host, "*.skype.com"))
	|| (shExpMatch(host, "*.skypeforbusiness.com"))
	|| (shExpMatch(host, "*.svc.ms"))
	|| (shExpMatch(host, "*.sway.com"))
	|| (shExpMatch(host, "*.symcd.com"))
	|| (shExpMatch(host, "*.teams.microsoft.com"))
	|| (shExpMatch(host, "*.tenor.com"))
	|| (shExpMatch(host, "*.urlp.sfbassets.com"))
	|| (shExpMatch(host, "*.users.storage.live.com"))
	|| (shExpMatch(host, "*.verisign.com"))
	|| (shExpMatch(host, "*.verisign.net"))
	|| (shExpMatch(host, "*.windows.net"))
	|| (shExpMatch(host, "*.windowsazure.com"))
	|| (shExpMatch(host, "*.wns.windows.com"))
	|| (shExpMatch(host, "*.wunderlist.com"))
	|| (shExpMatch(host, "*.yammer.com"))
	|| (shExpMatch(host, "*.yammerusercontent.com"))
	|| (shExpMatch(host, "*.hip.live.com"))
	|| (shExpMatch(host, "*.microsoftstream.com"))
	|| (shExpMatch(host, "*.microsoftusercontent.com"))
	|| (shExpMatch(host, "*.virtualearth.net"))
	|| (shExpMatch(host, "*.service.signalr.net"))
	|| (shExpMatch(host, "aad.portal.azure.com"))
	|| (shExpMatch(host, "account.activedirectory.windowsazure.com"))
	|| (shExpMatch(host, "account.live.com"))
	|| (shExpMatch(host, "acompli.helpshift.com"))
	|| (shExpMatch(host, "acompli-android-logs.s3.amazonaws.com"))
	|| (shExpMatch(host, "ad.atdmt.com"))
	|| (shExpMatch(host, "admin.onedrive.com"))
	|| (shExpMatch(host, "ajax.aspnetcdn.com"))
	|| (shExpMatch(host, "ajax.googleapis.com"))
	|| (shExpMatch(host, "aka.ms"))
	|| (shExpMatch(host, "amp.azure.net"))
	|| (shExpMatch(host, "analytics.localytics.com"))
	|| (shExpMatch(host, "api.dropboxapi.com"))
	|| (shExpMatch(host, "api.localytics.com"))
	|| (shExpMatch(host, "api.login.yahoo.com"))
	|| (shExpMatch(host, "apis.live.net"))
	|| (shExpMatch(host, "app.adjust.com"))
	|| (shExpMatch(host, "appexsin.stb.s-msn.com"))
	|| (shExpMatch(host, "apps.identrust.com"))
	|| (shExpMatch(host, "assets.onestore.ms"))
	|| (shExpMatch(host, "auth.gfx.ms"))
	|| (shExpMatch(host, "autologon.microsoftazuread-sso.com"))
	|| (shExpMatch(host, "az416426.vo.msecnd.net"))
	|| (shExpMatch(host, "by.uservoice.com"))
	|| (shExpMatch(host, "c.bing.com"))
	|| (shExpMatch(host, "c.bing.net"))
	|| (shExpMatch(host, "cacerts.digicert.com"))
	|| (shExpMatch(host, "cdn.odc.officeapps.live.com"))
	|| (shExpMatch(host, "cdn.onenote.net"))
	|| (shExpMatch(host, "cdn.optimizely.com"))
	|| (shExpMatch(host, "cdp1.public-trust.com"))
	|| (shExpMatch(host, "cert.int-x3.letsencrypt.org"))
	|| (shExpMatch(host, "cgn-production-cog-media.s3.amazonaws.com"))
	|| (shExpMatch(host, "cgn-production-cog-media.s3-us-west-2.amazonaws.com"))
	|| (shExpMatch(host, "cgn-production-static.s3.amazonaws.com"))
	|| (shExpMatch(host, "cgn-production-static.s3-us-west-2.amazonaws.com"))
	|| (shExpMatch(host, "cl2.apple.com"))
	|| (shExpMatch(host, "classiccompute.hosting.portal.azure.net"))
	|| (shExpMatch(host, "com.microsoft.office.outlook.dev"))
	|| (shExpMatch(host, "compliance.outlook.com"))
	|| (shExpMatch(host, "compute.hosting.portal.azure.net"))
	|| (shExpMatch(host, "crl.globalsign.com"))
	|| (shExpMatch(host, "crl.globalsign.net"))
	|| (shExpMatch(host, "crl.identrust.com"))
	|| (shExpMatch(host, "crl3.digicert.com"))
	|| (shExpMatch(host, "crl4.digicert.com"))
	|| (shExpMatch(host, "cus-odc.officeapps.live.com"))
	|| (shExpMatch(host, "cus-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "d.docs.live.net"))
	|| (shExpMatch(host, "data.flurry.com"))
	|| (shExpMatch(host, "dc.services.visualstudio.com"))
	|| (shExpMatch(host, "directory.services.live.com"))
	|| (shExpMatch(host, "docs.live.net"))
	|| (shExpMatch(host, "ea-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "ea-config.officeapps.live.com"))
	|| (shExpMatch(host, "ea-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "ecn.dev.virtualearth.net"))
	|| (shExpMatch(host, "enterpriseenrollment.boe.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.mfc.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.nedbank.co.ke"))
	|| (shExpMatch(host, "enterpriseenrollment.nedbank.co.uk"))
	|| (shExpMatch(host, "enterpriseenrollment.nedbank.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.nedbankprivatewealth.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.nedgroupinsurance.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.nedgroupinvestments.co.za"))
	|| (shExpMatch(host, "enterpriseenrollment.nedgrouplife.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.boe.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.mfc.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.nedbank.co.ke"))
	|| (shExpMatch(host, "enterpriseregistration.nedbank.co.uk"))
	|| (shExpMatch(host, "enterpriseregistration.nedbank.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.nedbankprivatewealth.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.nedgroupinsurance.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.nedgroupinvestments.co.za"))
	|| (shExpMatch(host, "enterpriseregistration.nedgrouplife.co.za"))
	|| (shExpMatch(host, "en-US.appex-rf.msn.com"))
	|| (shExpMatch(host, "eus2-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "eus2-config.officeapps.live.com"))
	|| (shExpMatch(host, "eus2-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "eus-odc.officeapps.live.com"))
	|| (shExpMatch(host, "eus-www.sway-cdn.com"))
	|| (shExpMatch(host, "eus-www.sway-extensions.com"))
	|| (shExpMatch(host, "excelbingmap.firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "excelcs.officeapps.live.com"))
	|| (shExpMatch(host, "firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "foodanddrink.services.appex.bing.com"))
	|| (shExpMatch(host, "g.live.com"))
	|| (shExpMatch(host, "graph.facebook.com"))
	|| (shExpMatch(host, "groupsapi2-prod.outlookgroups.ms"))
	|| (shExpMatch(host, "groupsapi3-prod.outlookgroups.ms"))
	|| (shExpMatch(host, "groupsapi4-prod.outlookgroups.ms"))
	|| (shExpMatch(host, "groupsapi-prod.outlookgroups.ms"))
	|| (shExpMatch(host, "hosting.portal.azure.net"))
	|| (shExpMatch(host, "hybridconfiguration.azurewebsites.net"))
	|| (shExpMatch(host, "img-prod-cms-rt-microsoft-com.akamaized.net"))
	|| (shExpMatch(host, "ind.delve.office.co"))
	|| (shExpMatch(host, "isb.ourservicedesk.com"))
	|| (shExpMatch(host, "isrg.trustid.ocsp.identrust.com"))
	|| (shExpMatch(host, "liverdcxstorage.blob.core.windowsazure.com"))
	|| (shExpMatch(host, "login.live.com"))
	|| (shExpMatch(host, "login.microsoftonline-p.com"))
	|| (shExpMatch(host, "login.windows-ppe.net"))
	|| (shExpMatch(host, "m.webtrends.com"))
	|| (shExpMatch(host, "management.azure.com"))
	|| (shExpMatch(host, "mem.gfx.ms"))
	|| (shExpMatch(host, "microsoft.com"))
	|| (shExpMatch(host, "mrodevicemgr.officeapps.live.com"))
	|| (shExpMatch(host, "ms.tific.com"))
	|| (shExpMatch(host, "msappproxy.net"))
	|| (shExpMatch(host, "ncus-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "ncus-odc.officeapps.live.com"))
	|| (shExpMatch(host, "ncus-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "nedbank.mail.protection.outlook.com"))
	|| (shExpMatch(host, "nedgroup-wealth-dynamics365.azurewebsites.net"))
	|| (shExpMatch(host, "neu-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "neu-config.officeapps.live.com"))
	|| (shExpMatch(host, "neu-odc.officeapps.live.com"))
	|| (shExpMatch(host, "neu-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "nexus.officeapps.live.com"))
	|| (shExpMatch(host, "nexusrules.officeapps.live.com"))
	|| (shExpMatch(host, "nps.onyx.azure.net"))
	|| (shExpMatch(host, "ocsa.officeapps.live.com"))
	|| (shExpMatch(host, "ocsp.digicert.com"))
	|| (shExpMatch(host, "ocsp.globalsign.com"))
	|| (shExpMatch(host, "ocsp.int-x3.letsencrypt.org"))
	|| (shExpMatch(host, "ocsp.msocsp.com"))
	|| (shExpMatch(host, "ocsp2.globalsign.com"))
	|| (shExpMatch(host, "ocspx.digicert.com"))
	|| (shExpMatch(host, "ocsredir.officeapps.live.com"))
	|| (shExpMatch(host, "ocws.officeapps.live.com"))
	|| (shExpMatch(host, "odc.officeapps.live.com"))
	|| (shExpMatch(host, "odcsm.officeapps.live.com"))
	|| (shExpMatch(host, "office.live.com"))
	|| (shExpMatch(host, "officeapps.live.com"))
	|| (shExpMatch(host, "officecdn.microsoft.com.edgekey.net"))
	|| (shExpMatch(host, "officecdn.microsoft.com.edgesuite.net"))
	|| (shExpMatch(host, "officeimg.vo.msecnd.net"))
	|| (shExpMatch(host, "ols.officeapps.live.com"))
	|| (shExpMatch(host, "oneclient.sfx.ms"))
	|| (shExpMatch(host, "outlook.uservoice.com"))
	|| (shExpMatch(host, "p100-sandbox.itunes.apple.com"))
	|| (shExpMatch(host, "partnerservices.getmicrosoftkey.com"))
	|| (shExpMatch(host, "peoplegraph.firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "platform.linkedin.com"))
	|| (shExpMatch(host, "policy.hosting.portal.azure.net"))
	|| (shExpMatch(host, "policykeyservice.dc.ad.msft.net"))
	|| (shExpMatch(host, "portal.azure.com"))
	|| (shExpMatch(host, "pptcs.officeapps.live.com"))
	|| (shExpMatch(host, "prod.firstpartyapps.oaspapps.com.akadns.net"))
	|| (shExpMatch(host, "quicktips.skypeforbusiness.com"))
	|| (shExpMatch(host, "rink.hockeyapp.net"))
	|| (shExpMatch(host, "roaming.officeapps.live.com"))
	|| (shExpMatch(host, "s.ytimg.com"))
	|| (shExpMatch(host, "s0.assets-yammer.com"))
	|| (shExpMatch(host, "s-0001.s-msedge.net"))
	|| (shExpMatch(host, "s-0004.s-msedge.net"))
	|| (shExpMatch(host, "s3-us-west-2.amazonaws.com"))
	|| (shExpMatch(host, "sas.officeapps.live.com"))
	|| (shExpMatch(host, "scsinstrument-ss-us.trafficmanager.net"))
	|| (shExpMatch(host, "scsquery-ss-asia.trafficmanager.net"))
	|| (shExpMatch(host, "scsquery-ss-eu.trafficmanager.net"))
	|| (shExpMatch(host, "scsquery-ss-us.trafficmanager.net"))
	|| (shExpMatch(host, "scus-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "scus-config.officeapps.live.com"))
	|| (shExpMatch(host, "scus-odc.officeapps.live.com"))
	|| (shExpMatch(host, "scus-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "sdk.hockeyapp.net"))
	|| (shExpMatch(host, "sea-config.officeapps.live.com"))
	|| (shExpMatch(host, "sea-odc.officeapps.live.com"))
	|| (shExpMatch(host, "sea-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "secure.aadcdn.microsoftonline-p.com"))
	|| (shExpMatch(host, "secure.globalsign.com"))
	|| (shExpMatch(host, "shellprod.msocdn.com"))
	|| (shExpMatch(host, "signup.live.com"))
	|| (shExpMatch(host, "site-cdn.onenote.net"))
	|| (shExpMatch(host, "skydrive.wns.windows.com"))
	|| (shExpMatch(host, "skypemaprdsitus.trafficmanager.net"))
	|| (shExpMatch(host, "social.yahooapis.com"))
	|| (shExpMatch(host, "spoprod-a.akamaihd.net"))
	|| (shExpMatch(host, "ssw.live.com"))
	|| (shExpMatch(host, "staffhub.ms"))
	|| (shExpMatch(host, "staffhub.uservoice.com"))
	|| (shExpMatch(host, "statics-marketingsites-neu-ms-com.akamaized.net"))
	|| (shExpMatch(host, "storage.live.com"))
	|| (shExpMatch(host, "sts.nedsecure.co.za"))
	|| (shExpMatch(host, "sway.com"))
	|| (shExpMatch(host, "telemetry.remoteapp.windowsazure.com"))
	|| (shExpMatch(host, "telemetryservice.firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "tse1.mm.bing.net"))
	|| (shExpMatch(host, "uci.officeapps.live.com"))
	|| (shExpMatch(host, "us-central1-sintrexapp.cloudfunctions.net"))
	|| (shExpMatch(host, "view.atdmt.com"))
	|| (shExpMatch(host, "weather.tile.appex.bing.com"))
	|| (shExpMatch(host, "web.localytics.com"))
	|| (shExpMatch(host, "webanalytics.localytics.com"))
	|| (shExpMatch(host, "weu-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "weu-config.officeapps.live.com"))
	|| (shExpMatch(host, "weu-odc.officeapps.live.com"))
	|| (shExpMatch(host, "weu-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "wikipedia.firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "wns.windows.com"))
	|| (shExpMatch(host, "wordcs.officeapps.live.com"))
	|| (shExpMatch(host, "wus-000.ocws.officeapps.live.com"))
	|| (shExpMatch(host, "wus-config.officeapps.live.com"))
	|| (shExpMatch(host, "wus-firstpartyapps.oaspapps.com"))
	|| (shExpMatch(host, "wus-odc.officeapps.live.com"))
	|| (shExpMatch(host, "wus-roaming.officeapps.live.com"))
	|| (shExpMatch(host, "wus-www.sway-cdn.com"))
	|| (shExpMatch(host, "wus-www.sway-extensions.com"))
	|| (shExpMatch(host, "www.acompli.com"))
	|| (shExpMatch(host, "www.bing.com"))
	|| (shExpMatch(host, "www.digicert.com"))
	|| (shExpMatch(host, "www.evernote.com"))
	|| (shExpMatch(host, "www.google-analytics.com"))
	|| (shExpMatch(host, "www.googleapis.com"))
	|| (shExpMatch(host, "www.onedrive.com"))
	|| (shExpMatch(host, "www.remoteapp.windowsazure.com"))
	|| (shExpMatch(host, "api.aadr.mspim.azure.com"))
	|| (shExpMatch(host, "api.azrbac.mspim.azure.com"))
	|| (shExpMatch(host, "api.meetup.com"))
	|| (shExpMatch(host, "c.live.com"))
	|| (shExpMatch(host, "officespeech.platform.bing.com"))
	|| (shExpMatch(host, "secure.meetup.com"))
	|| (shExpMatch(host, "*.outlookmobile.com"))
	|| (shExpMatch(host, "*.azure.net"))
	|| (shExpMatch(host, "*.azure.com"))
	|| (shExpMatch(host, "*powerpoint-podsws.officeapps.live.com"))
	|| (shExpMatch(host, "*rtc.officeapps.live.com"))
	|| (shExpMatch(host, "*shared.officeapps.live.com"))
	|| (shExpMatch(host, "delve-gcc.office.com"))
	|| (shExpMatch(host, "*.events.data.microsoft.com"))
	|| (shExpMatch(host, "*.msauth.net"))
	|| (shExpMatch(host, "*.msauthimages.net"))
	|| (shExpMatch(host, "*.msftauth.net"))
	|| (shExpMatch(host, "*.msftauthimages.net"))
	|| (shExpMatch(host, "*.measure.office.com"))
	|| (shExpMatch(host, "*.msftidentity.com"))
	|| (shExpMatch(host, "*.msidentity.com"))
	|| (shExpMatch(host, "*.informationprotection.azure.com"))
	|| (shExpMatch(host, "yammer.com")))
       	{
         return "PROXY 10.59.8.210:80";
     	}



        if ((host == "196.6.238.100") ||
	   (host == "196.6.239.100"))
	{return "PROXY 172.17.2.9:80; PROXY 172.17.2.10:80; PROXY 172.17.2.11:80";}

        if (dnsDomainIs(host, ".cqgnet.com") ||
           dnsDomainIs(host, ".sscportia.com") ||
           dnsDomainIs(host, ".360t.com") ||
           dnsDomainIs(host, ".citifxvelocity.com") ||
           dnsDomainIs(host, ".jpmorgan.com") ||
           dnsDomainIs(host, ".barcap.com") ||
           dnsDomainIs(host, ".cqg.com") ||
           dnsDomainIs(host, ".currenex.net") ||
           dnsDomainIs(host, ".thomsonone.com") ||
           (host == "pfm.nedbank.co.za") ||
           (host == "www.myfinanciallife.co.za") ||
	   (host == "159.220.63.14") ||
	   (host == "159.220.63.15") ||
	   (host == "159.220.63.16") ||
	   (shExpMatch(host, "nedbank.dealogic.com")) ||
	   dnsDomainIs(host, ".bgctrader.com") ||
           (host == "myfinanciallife.nedsecure.co.za"))
        {return "PROXY 172.25.16.11:80; PROXY 172.25.16.12:80";}

        if (ipaddr.test(host))
         {if (isInNet(host, "127.0.0.0", "255.0.0.0") ||
            isInNet(host, "10.0.0.0", "255.0.0.0") ||
            isInNet(host, "163.199.48.0", "255.255.255.224") ||
            isInNet(host, "168.142.200.0", "255.255.255.0") ||
            isInNet(host, "168.142.246.0", "255.255.255.0") ||
            isInNet(host, "168.142.247.0", "255.255.255.192") ||
            isInNet(host, "172.16.0.0", "255.240.0.0") ||
            isInNet(host, "172.24.0.0", "255.255.0.0") ||
            isInNet(host, "192.168.0.0", "255.255.0.0") ||
            isInNet(host, "196.1.110.0", "255.255.255.192") ||
            isInNet(host, "196.3.0.0", "255.255.0.0") ||
            isInNet(host, "196.6.0.0", "255.255.0.0") ||
            isInNet(host, "196.14.0.0", "255.255.0.0") ||
            isInNet(host, "196.31.0.0", "255.255.0.0") ||
            isInNet(host, "196.33.158.192", "255.255.255.240") ||
            isInNet(host, "196.33.199.0", "255.255.255.0") ||
            isInNet(host, "196.33.252.0", "255.255.255.0") ||
            isInNet(host, "196.33.255.128", "255.255.255.224") ||
            isInNet(host, "196.35.0.0", "255.255.0.0") ||
            isInNet(host, "196.36.0.0", "255.255.0.0") ||
            isInNet(host, "196.38.0.0", "255.255.0.0") ||
            isInNet(host, "196.11.208.0" , "255.255.255.0") ||
            isInNet(host, "168.142.125.0", "255.255.255.192") ||
            isInNet(host, "168.168.57.0", "255.255.255.192") ||
            isInNet(host, "206.182.47.0", "255.255.255.128"))
         {return "DIRECT";}
         }



        if (res_c_ip == 0)
           {return proxy_0;}
             else if (res_c_ip == 1)
           {return proxy_1;}
             else if (res_c_ip == 2)
           {return proxy_2;}

}
 
        function URLhash(name)
        {var cnt = 0;
         var str = name.toLowerCase(name);
    
         if (str.length == 0)
          {return cnt;}
    
         for (var i = 0; i < str.length; i++) 
          {var ch = atoi(str.substring(i, i + 1));
          cnt = cnt + ch;
          }
   
          return cnt;
        }

        function atoi(charstring)
           {if ( charstring == "a" ) return 0x61; if ( charstring == "b" ) return 0x62;
           if ( charstring == "c" ) return 0x63; if ( charstring == "d" ) return 0x64;
           if ( charstring == "e" ) return 0x65; if ( charstring == "f" ) return 0x66;
           if ( charstring == "g" ) return 0x67; if ( charstring == "h" ) return 0x68;
           if ( charstring == "i" ) return 0x69; if ( charstring == "j" ) return 0x6a;
           if ( charstring == "k" ) return 0x6b; if ( charstring == "l" ) return 0x6c;
           if ( charstring == "m" ) return 0x6d; if ( charstring == "n" ) return 0x6e;
           if ( charstring == "o" ) return 0x6f; if ( charstring == "p" ) return 0x70;
           if ( charstring == "q" ) return 0x71; if ( charstring == "r" ) return 0x72;
           if ( charstring == "s" ) return 0x73; if ( charstring == "t" ) return 0x74;
           if ( charstring == "u" ) return 0x75; if ( charstring == "v" ) return 0x76;
           if ( charstring == "w" ) return 0x77; if ( charstring == "x" ) return 0x78;
           if ( charstring == "y" ) return 0x79; if ( charstring == "z" ) return 0x7a;
           if ( charstring == "0" ) return 0x30; if ( charstring == "1" ) return 0x31;
           if ( charstring == "2" ) return 0x32; if ( charstring == "3" ) return 0x33;
           if ( charstring == "4" ) return 0x34; if ( charstring == "5" ) return 0x35;
           if ( charstring == "6" ) return 0x36; if ( charstring == "7" ) return 0x37;
           if ( charstring == "8" ) return 0x38; if ( charstring == "9" ) return 0x39;
           if ( charstring == "." ) return 0x2e; return 0x20;}
