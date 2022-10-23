/**
 * 
 */
package com.nextlabs.ac;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.IEvalValue;
import com.bluejungle.framework.expressions.IMultivalue;
import com.bluejungle.framework.expressions.Multivalue;
import com.bluejungle.framework.expressions.ValueType;
import com.bluejungle.pf.domain.destiny.serviceprovider.ISubjectAttributeProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.bluejungle.pf.domain.destiny.subject.IDSubject;
import com.nextlabs.ac.helper.ACConstants;
import com.nextlabs.ac.helper.HSQLHelper;
import com.nextlabs.ac.helper.PropertyLoader;
import com.nextlabs.ac.helper.QueryBuilder;
import com.nextlabs.ac.helper.QueryResultHelper;
import com.nextlabs.ac.helper.Utils;

/**
 * @author snehru
 * 
 */
public class UserAttrProvider implements ISubjectAttributeProvider {

	private static Properties prop = PropertyLoader

	.loadProperties(ACConstants.COMMON_AC_PROPFILE);

	private static IEvalValue nullResult = IEvalValue.NULL;
	// UTC test Case 5: Bug fix start
	private static IEvalValue emptySet = EvalValue.build(Multivalue.EMPTY);
	// UTC test Case 5: Bug fix end
	// private static HashSet<String> userTableColumnSet;

	private static HSQLHelper hsqlHelper;

	protected String userIDtype;// Assigned either windowsSID or UserID

	private final static String windows_SID = prop

	.getProperty("user_col_WindowsSID");
	private static IEvalValue emptyString = EvalValue.build("");
	private static String LICENSESTATUS;

	private static String LICENSESQLOPERATOR;
	private final static String user_ID = prop.getProperty("user_col_UserID");
	private final static String AuthIDList = "AuthIDList";
	private final static String ValidAuthIDList = "ValidAuthIDList";

	// Name of the

	// tables(User,UserCountryMapping,UserAuthorityMapping,AuthorityAuhtorizedUserMapping,Authority)

	// is loaded from properties.

	private final static String user = prop.getProperty("table_user");

	/*
	 * 
	 * global default values which has to be assigned during the initiation of
	 * 
	 * the service
	 */
	public static ThreadLocal<HashMap<String, Object>> userObject = new ThreadLocal<HashMap<String, Object>>() {
		@Override
		protected HashMap<String, Object> initialValue() {
			return null;
		}
	};

	public void init() throws Exception {

		LOG.info("UserAttributePlugin init() started.");

		if (null != prop) {

			hsqlHelper = new HSQLHelper(prop.getProperty("hsql_server_url"),
					prop.getProperty("hsql_user_name"),
					prop.getProperty("hsql_password"), Integer.parseInt(prop
							.getProperty("hsql_connectionpool_size")));
			LOG.info("countryofcompany:" + prop.getProperty("countryofcompany"));
		} else {
			LOG.info("UserAttributePlugin init() failed to establish a connection with HSQL.");
		}

		LOG.info("UserAttributePlugin init() completed.");

	}

	@Override
	public IEvalValue getAttribute(IDSubject arg0, String arg1)
			throws ServiceProviderException {
		long lCurrentTime = System.nanoTime();
		String id = arg0.getUid();

			LOG.info("PWCUA userid:" + id);
			LOG.info("PWCUA attribute:" + arg1);

		ArrayList<String> inputList = new ArrayList<String>();
		inputList.add(id);
		IEvalValue value = IEvalValue.NULL;
		try {

			if (arg1.equalsIgnoreCase("licenses")) {
				value = getLicenses(inputList, false);
			} else if (arg1.equalsIgnoreCase("validlicenses")) {
				value = getLicenses(inputList, true);
			} else if (arg1.equalsIgnoreCase("id")) {
				value = EvalValue.build(id);
			} else if (arg1.equalsIgnoreCase("internallicensenos")) {
				value = getInternalLicenseNos(inputList, false);
			} else if (arg1.equalsIgnoreCase("validinternallicensenos")) {
				value = getInternalLicenseNos(inputList, true);
			} else if (arg1.equalsIgnoreCase("itarcountries")) {
				inputList.add("ITAR");
				value = getAllCountries(inputList);
			} else if (arg1.equalsIgnoreCase("earcountries")) {
				inputList.add("EAR");
				value = getAllCountries(inputList);
			} else if (arg1.equalsIgnoreCase("eclcountries")) {
				inputList.add("ECL");
				value = getAllCountries(inputList);
			} else {
				inputList.add(arg1);
				value = getAttribute(inputList);
			}
		} catch (Exception e) {

			LOG.error("UtcUserAttributePlugin getAttribute() error: ", e);
			value = IEvalValue.NULL;
		}
		LOG.info("UtcUserAttributePlugin getAttribute() completed. " + arg1
				+ ": "

				+ value.getValue() + " Time spent: "

				+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		return value;
	}

	/*
	 * Basic validation of user sevice is done here. This method is added as a
	 * part of UTC testcase 5 bug fix
	 */
	private boolean basicValidation(ArrayList<String> inputList) {
		if (inputList.size() > 0) {
			if (!inputList.get(0).isEmpty()) {
				identifyUserIdType(inputList.get(0));
				return true;
			} else {
				LOG.error("Error:Incorrect value for UserID in the parameter list ");
				return false;
			}
		} else {
			LOG.error("Error:Incorrect no of parameters ");
			return false;
		}

	}

	/*
	 * 
	 * This method is called from callfunction to get the internalLicenseno of
	 * 
	 * the given user
	 */

	private IEvalValue getInternalLicenseNos(ArrayList<String> inputList,
			boolean validFlag) {

		LOG.info("UtcUserAttributePlugin getIntenalLicenseNos() started");

		if (!basicValidation(inputList))
			return emptySet;
		if (inputList.size() < 1) {

			LOG.warn("Error:Incorrect no of parameters ");

			return emptySet;

		}

		String userID = inputList.get(0);

		QueryBuilder query = prepareQueryForInternalLicenseNos(prepareQueryForLicenses(
				userID, validFlag));

		ArrayList<String> internalLicenseNoList = hsqlHelper

		.getGenAttributeList(query);

		if (internalLicenseNoList != null) {

			return prepareIEmultivalueFromListofString(internalLicenseNoList);

		} else {

			return emptySet;

		}

	}

	/*
	 * 
	 * Prepare IMultiValue result from the given list for strings
	 */

	private IEvalValue prepareIEmultivalueFromListofString(

	ArrayList<String> valueList) {

		IEvalValue evalvalue = emptySet;

		if (valueList != null && valueList.size() > 0) {
			IMultivalue imv = Multivalue.create(valueList, ValueType.STRING);

			evalvalue = EvalValue.build(imv);
		}

		return evalvalue;

	}

	/*
	 * 
	 * Identifies whether the User ID is WindowsSID or UserID
	 */

	protected void identifyUserIdType(String id) {

		LOG.info("UtcUserAttributePlugin identifyUserIdType() started");

		Pattern p = Pattern.compile("^[Ss]-[0-9]{1}-[0-9]{1}([0-9]|-)*$");

		Matcher m = p.matcher(id);

		if (m.matches()) {

			userIDtype = windows_SID;

		} else {

			userIDtype = user_ID;

		}

	}

	protected ArrayList<String> getGenAttributeList(QueryBuilder qb)

	{

		LOG.info("UtcUserAttributePlugin getGenAttributeList() started. This is called from BizAuthService");

		return hsqlHelper.getGenAttributeList(qb);

	}

	/*
	 * 
	 * This method is called from the callfuntion for getting all countries of
	 * 
	 * the User
	 */

	private IEvalValue getAllCountries(ArrayList<String> inputList) {

		LOG.info("UtcUserAttributePlugin getAllCountries() started");
		if (!basicValidation(inputList))
			return emptySet;
		if (inputList.size() < 2) {
			LOG.warn("Error:Incorrect no of parameters ");
			return emptySet;
		}

		String userID = "'" + inputList.get(0) + "'";
		String jurisdiction = inputList.get(1);
		String countryType = "";
		// boolean isPhysicalLocationAvailable=false;
		String separator = prop.getProperty("get_all_country_separator");
		if (jurisdiction != null && !jurisdiction.isEmpty()
				&& separator != null) {
			countryType = prop.getProperty(jurisdiction);
			if (countryType != null) {
				StringTokenizer st = new StringTokenizer(countryType, separator);
				StringBuilder sb = new StringBuilder("(");
				while (st.hasMoreTokens()) {
					String type = st.nextToken();
					/*
					 * if(type.equalsIgnoreCase(prop.getProperty("physical_location"
					 * ))) { isPhysicalLocationAvailable=true; }
					 */
					sb.append("'");
					sb.append(type.toLowerCase().trim());
					sb.append("'");
					if (st.hasMoreTokens())
						sb.append(",");
				}
				sb.append(")");
				countryType = sb.toString();
			} else {
				LOG.warn("you might have missed the entry in the common_ac property file for '"
						+ jurisdiction + "'");
				return emptySet;
			}

		} else {
			LOG.warn("Error:Incorrect Jurisdiction type");
			return emptySet;
		}
		// String physicalLoaction = inputList.get(2);

		QueryBuilder query = prepareAllCountryQuery(userID, countryType);
		ArrayList<String> countryList = hsqlHelper.getGenAttributeList(query);
		if (countryList != null) {
			/*
			 * if (physicalLoaction != null && !physicalLoaction.isEmpty()) { if
			 * (!countryList.contains(physicalLoaction))
			 * countryList.add(physicalLoaction); }
			 */
			return prepareIEmultivalueFromListofString(countryList);
		} else {
			return emptySet;
		}
	}

	/*
	 * 
	 * This method is called from the CallFuntion to get all authorityIDs
	 * 
	 * associated with the user
	 */

	private IEvalValue getLicenses(ArrayList<String> inputList,
			boolean validFlag) {

		LOG.info("UtcUserAttributePlugin getLicenses() started");

		if (!basicValidation(inputList))
			return emptySet;

		if (inputList.size() < 1) {

			LOG.error("Error:Incorrect no of parameters ");

			return emptySet;

		}

		String userID = inputList.get(0);
		if (userObject.get() == null) {
			LOG.info("Inside UserObject get licenses");
			setUser(userID, true, true, true);
		}
		if (!userID.equalsIgnoreCase((String) userObject.get().get(userIDtype))) {
			LOG.info("Inside UserObject get Attribute");
			setUser(userID, true, true, true);
		}
		if (userObject.get() != null) {

			/*
			 * QueryBuilder query = prepareQueryForLicenses(userID);
			 * 
			 * ArrayList<String> authorityIDList = hsqlHelper
			 * .getGenAttributeList(query);
			 */
			ArrayList<String> authorityIDList = null;
			if (userObject.get().get(AuthIDList) != null) {
				if (validFlag) {
					authorityIDList = (ArrayList<String>) userObject.get().get(
							ValidAuthIDList);
				} else {
					authorityIDList = (ArrayList<String>) userObject.get().get(
							AuthIDList);
				}
			}

			if (authorityIDList != null) {

				return prepareIEmultivalueFromListofString(authorityIDList);

			}
		}
		return emptySet;
	}

	/*
	 * 
	 * Prepares a query for getting all countries from usercountrymappingtable
	 */

	protected QueryBuilder prepareAllCountryQuery(String userID,
			String countryType) {

		LOG.info("UtcUserAttributePlugin prepareAllCountryQuery() called");

		QueryBuilder qb = new QueryBuilder();

		String query = "SELECT DISTINCT(uc.{0}) FROM {1} u,{2} uc WHERE  u.{3}=uc.{3} AND LCASE(TRIM({3}))=LCASE(TRIM({4})) AND LCASE(TRIM({5})) IN {6}";

		Object[] tcargsForQuery = { prop.get("ucm_col_countrycode"), user,

		prop.get("table_ucm"), userIDtype, userID, prop.get("ucm_col_type"),
				countryType };

		qb.setPreparedQuery(MessageFormat.format(query, tcargsForQuery));

		LOG.info("UtcUserAttributePlugin Query for gettting AllCountries : "

		+ qb.getPreparedQuery());

		return qb;

	}

	/*
	 * 
	 * Prepares a query for getting the authorityid by joining the
	 * 
	 * AuthorityAuthorizedUserMapping table and UserAuthorityTable Mapping table
	 * 
	 * for the given user
	 */

	private QueryBuilder prepareQueryForLicenses(String userId,
			boolean validFlag) {

		String otherIDType = getOtherIDType();
		LOG.info("userid:" + userId + "otherIDType" + otherIDType);

		if (userObject.get() == null) {
			LOG.info("Inside UserObject get licenses");
			setUser(userId, true, true, true);
		}

		if (userObject.get() != null
				&& userObject.get().get(userIDtype) != null && !userId.equalsIgnoreCase((String) userObject.get().get(userIDtype))) {
			LOG.info("Inside UserObject get Attribute");
			setUser(userId, true, true, true);
		}
		String otherIDValue = "";
		// Suspect No -1 for null pointer exception
		if (userObject.get() != null) {
			otherIDValue = userObject.get().get(otherIDType).toString();
		}

		QueryBuilder qb = new QueryBuilder();

		String query = "SELECT {0} FROM {1}  WHERE {2}={3} OR {4}={5} UNION SELECT {0} FROM {6} WHERE {2}={3}";
		if (validFlag) {
			query = "SELECT {0} FROM {12} WHERE {0} in ("
					+ query
					+ ") AND LCASE(TRIM({7})) {8} {9} AND   {10} < current_date AND  {11} > current_date";
		}

		Object[] tcargsForQuery = { prop.get("au_col_AuthorityId"),

		prop.get("table_aaum"), userIDtype,

		"'" + userId + "'", otherIDType, "'" + otherIDValue + "'",
				prop.get("table_uam"),
				prop.getProperty("au_col_LicenseStatus"), LICENSESQLOPERATOR,
				LICENSESTATUS, prop.getProperty("au_col_StartDate"),

				prop.getProperty("au_col_EndDate"), prop.get("table_au") };

		qb.setPreparedQuery(MessageFormat.format(query, tcargsForQuery));

		LOG.info("UtcUserAttributePlugin Query for gettting all authorityIDs : "

				+ qb.getPreparedQuery());

		return qb;

	}

	/*
	 * 
	 * Prepares a query for getting the internal license nos from the authority
	 * 
	 * table for the given user
	 */

	public String getOtherIDType() {
		if (userIDtype!=null && userIDtype.equals(user_ID))
			return windows_SID;
		else
			return user_ID;
	}

	/*
	 * This method help to load the license status from properites. The License
	 * status SQL operator is decided by the LicenseStatus values.
	 */
	private void setLicenseStatus() {
		String licenseStatusVals = prop.getProperty("license_status_active");
		String licenseStatusSepr = prop.getProperty("license_status_separator");
		if (licenseStatusVals != null && licenseStatusSepr != null) {
			StringTokenizer strToken = new StringTokenizer(licenseStatusVals,
					licenseStatusSepr);
			if (strToken.countTokens() == 1) {
				LICENSESQLOPERATOR = "=";
				LICENSESTATUS = "'" + strToken.nextToken().trim().toLowerCase()
						+ "'";
			} else if (strToken.countTokens() > 1) {
				LICENSESQLOPERATOR = "IN";
				StringBuilder sb = new StringBuilder("(");
				while (strToken.hasMoreTokens()) {
					sb.append("'");
					sb.append(strToken.nextToken().trim().toLowerCase());
					sb.append("'");
					if (strToken.hasMoreTokens()) {
						sb.append(",");
					}
				}
				sb.append(")");
				LICENSESTATUS = sb.toString();
			}
		}
		LOG.info("LICENSESQLOPERATOR:" + LICENSESQLOPERATOR);
		LOG.info("LICENSESTATUS:" + LICENSESTATUS);

	}

	private void setUser(String userid, boolean isCountry, boolean isMain,
			boolean isLicenses) {
		long lCurrentTime = System.nanoTime();
		HashMap<String, Object> tl = new HashMap<String, Object>();
		QueryResultHelper qrh = new QueryResultHelper();
		if (isMain) {

			qrh.setQuery(MessageFormat
					.format("SELECT * FROM {0}  WHERE LCASE(TRIM({1}))=LCASE(TRIM(''{2}''))",
							prop.getProperty("table_user"), userIDtype, userid));
			qrh.getTableFieldList()
					.add(prop.getProperty("user_col_WindowsSID"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_UserID"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_usertype"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_FirstName"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_LastName"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_USPerson"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_CompanyId"));
			qrh.getTableFieldList().add(prop.getProperty("user_col_CGDESS"));
			qrh.getTableFieldList().add(
					prop.getProperty("user_col_CGDEExpiration"));
			LOG.info(qrh.getQuery());
			tl = hsqlHelper.retrieveData(qrh);
			if (tl != null)
				userObject.set(tl);
			else
				LOG.warn("User Data is not enrolled or missing for userid:"
						+ userid);
			LOG.info("UtcUserAttributePlugin set user completed get a user. Result:  Time spent: "

					+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		}
		lCurrentTime = System.nanoTime();
		if (tl != null) {
			if (isCountry) {
				qrh.setTableFieldList(new ArrayList<String>());
				qrh.setQuery(MessageFormat
						.format("SELECT {0},{1} FROM {2} uc WHERE LCASE(TRIM({3}))=LCASE(TRIM(''{4}''))",
								prop.getProperty("ucm_col_countrycode"),
								prop.getProperty("ucm_col_type"),
								prop.getProperty("table_ucm"), userIDtype,
								userid));

				qrh.getTableFieldList().add(prop.getProperty("ucm_col_type"));
				qrh.getTableFieldList().add(
						prop.getProperty("ucm_col_countrycode"));
				HashMap<String, Object> userMap = hsqlHelper
						.retrieveCountries(qrh);
				LOG.info("getting coutry list:");
				LOG.info("userMap:" + userMap);

				try {
					if (userMap != null && userMap.size() > 0)
						tl.putAll(userMap);
					LOG.info("tl:" + tl);
				} catch (Exception ex) {
					LOG.error(
							" Error while adding user country list to the thread local",
							ex);
				}
				LOG.info("UtcUserAttributePlugin set user completed get a country. Result:  Time spent: "

						+ ((System.nanoTime() - lCurrentTime) / 1000000.00)
						+ "ms");
			}
			if (isLicenses) {
				setLicenseStatus();
				lCurrentTime = System.nanoTime();
				QueryBuilder query = prepareQueryForLicenses(userid, false);
				ArrayList<String> authorityIDList = hsqlHelper
						.getGenAttributeList(query);
				if (authorityIDList == null) {
					authorityIDList = new ArrayList<String>();
				}
				LOG.info("licenses: " + authorityIDList);
				tl.put(AuthIDList, authorityIDList);
				query = prepareQueryForLicenses(userid, true);
				ArrayList<String> validauthorityIDList = hsqlHelper
						.getGenAttributeList(query);
				if (validauthorityIDList == null) {
					validauthorityIDList = new ArrayList<String>();
				}
				LOG.info("validlicenses: " + validauthorityIDList);
				tl.put(ValidAuthIDList, validauthorityIDList);
				LOG.info("T1:" + tl);
				LOG.info("UtcUserAttributePlugin set user completed get licenses. Result:  Time spent: "

						+ ((System.nanoTime() - lCurrentTime) / 1000000.00)
						+ "ms");
			}
		}
		LOG.info("PWC UAP t1: " + tl);
		userObject.set(tl);

	}

	private QueryBuilder prepareQueryForInternalLicenseNos(QueryBuilder qb) {

		Object[] tcargsForQuery = { prop.get("au_col_InternalLicenseNo"),

		prop.get("table_au"), prop.get("au_col_AuthorityId") };

		StringBuilder query = new StringBuilder(MessageFormat.format(

		"SELECT DISTINCT({0}) FROM {1} WHERE {2} in (", tcargsForQuery));

		query.append(qb.getPreparedQuery());

		query.append(")");

		qb.setPreparedQuery(query.toString());

		LOG.info("UtcUserAttributePlugin Query for gettting all internalLicensenos : "

				+ qb.getPreparedQuery());

		return qb;

	}

	/*
	 * 
	 * This method is invoked by the callfunction to get an attribute of a user
	 * 
	 * from the DB
	 */

	private IEvalValue getAttribute(ArrayList<String> inputList) {

		LOG.info("UtcUserAttributePlugin getAttribute() started");

		try {
			if (!basicValidation(inputList))
				return emptyString;

			if (inputList.size() < 2) {
				LOG.warn("Error:Incorrect no of parameters ");
				return emptyString;
			}
			LOG.info("UtcUserAttributePlugin process the getattribute logic");
			IEvalValue evalvalue = emptyString;
			String userID = inputList.get(0);

			String attribute = inputList.get(1);
			if (userObject.get() == null) {
				LOG.info("Inside UserObject get Attribute");
				setUser(userID, true, true, true);
				LOG.info("USER TL" + userObject.get());
			}
			if (userObject.get() != null
					&& userObject.get().get(userIDtype) != null
					&& !userID.equalsIgnoreCase(userObject.get()
							.get(userIDtype).toString())) {
				LOG.info("Inside user id" + userObject.get() + " " + userID);
				setUser(userID, true, true, true);
			}
			if (userObject.get() != null) {
				LOG.info("Inside userObject.get() != null");
				LOG.info("Inside userObject:" + userObject.get());
				if (attribute.isEmpty()) {
					LOG.warn("Error:Incorrect value for the attribute ");
					return emptyString;
				}
				LOG.info("Inside Attribute:" + attribute.trim());
				LOG.info("Property file value:"
						+ prop.getProperty(attribute.trim()));
				LOG.info("The Attribute Value"
						+ userObject.get().get(
								prop.getProperty(attribute.trim())));

				Object attributeValue = userObject.get().get(
						prop.getProperty(attribute.trim()));

				if (attributeValue != null) {
					if ("cgdeexpiration".equalsIgnoreCase(attribute.trim())) {
						Date da = (Date) attributeValue;
						evalvalue = EvalValue.build(da);
					} else {
						evalvalue = EvalValue.build(attributeValue.toString());
					}

					LOG.info("UtcUserAttributePlugin getAttribute() returns "
							+ attributeValue);

				} else {
					LOG.info(attribute + " not found in the enrolled data");
					return emptyString;
				}

			}
			return evalvalue;
		} catch (Exception e) {
			LOG.info("Error while retriving attribute" + e.getMessage()
					+ ". So returing empty string");

		}
		return emptyString;
	}

	protected ArrayList<String> processValues(IEvalValue[] args) {

		int i = 0;

		ArrayList<String> sOutData = new ArrayList<String>();

		for (IEvalValue ieValue : args) {

			String sData = "";

			if (null != ieValue) {

				LOG.info("ieValue.getType()" + ieValue.getType());
				if (ieValue.getType() == ValueType.STRING) {

					sData = ieValue.getValue().toString();

				}

				LOG.info("----" + i + "." + sData + "-----");

				sOutData.add(sData);

			}

			i++;

		}

		return sOutData;

	}

	private static final Log LOG = LogFactory.getLog(UserAttrProvider.class);
	private static String INSTALL_LOC = "";
	private static final String PC_PATH = "/jservice/jar/ge/";
	private static final String FILE_NAME = "userlicenses.bin";
	private String licenseFilePath = "";

	private String findInstallFolder() {

		File f = new File(".");

		String path = f.getAbsolutePath();

		return path;

	}

}
