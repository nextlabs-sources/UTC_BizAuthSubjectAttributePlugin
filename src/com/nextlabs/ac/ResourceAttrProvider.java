/**
 * 
 */
package com.nextlabs.ac;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.bluejungle.framework.expressions.EvalValue;
import com.bluejungle.framework.expressions.IEvalValue;
import com.bluejungle.framework.expressions.IMultivalue;
import com.bluejungle.framework.expressions.Multivalue;
import com.bluejungle.framework.expressions.ValueType;
import com.bluejungle.pf.domain.destiny.serviceprovider.IResourceAttributeProvider;
import com.bluejungle.pf.domain.destiny.serviceprovider.ServiceProviderException;
import com.bluejungle.pf.domain.epicenter.resource.IResource;
import com.nextlabs.ac.helper.ACConstants;
import com.nextlabs.ac.helper.HSQLHelper;
import com.nextlabs.ac.helper.PropertyLoader;
import com.nextlabs.ac.helper.QueryBuilder;

/**
 * @author snehru
 * 
 */
public class ResourceAttrProvider implements IResourceAttributeProvider {

	private static final Log LOG = LogFactory
			.getLog(ResourceAttrProvider.class);
	private static Properties prop = PropertyLoader
			.loadProperties(ACConstants.COMMON_AC_PROPFILE);
	private static HSQLHelper hsqlHelper;

	private static String LICENSESTATUS;

	private static String LICENSESQLOPERATOR;
	// UTC test Case 5: Bug fix start
	private static IEvalValue emptySet = EvalValue.build(Multivalue.EMPTY);
	// UTC test Case 5: Bug fix end
	private final static String INTERNALLICENSEARGUMENT = MessageFormat.format(
			"DISTINCT({0})", prop.getProperty("au_col_InternalLicenseNo"));
	private final static String LICENSEARGUMENT = MessageFormat.format(
			"DISTINCT({0})", prop.getProperty("au_col_AuthorityId"));

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.bluejungle.pf.domain.destiny.serviceprovider.IServiceProvider#init()
	 */
	@Override
	public void init() throws Exception {
		LOG.info("ResourceService init() started.");

		if (null != prop) {
			hsqlHelper = new HSQLHelper(prop.getProperty("hsql_server_url"),
					prop.getProperty("hsql_user_name"),
					prop.getProperty("hsql_password"), Integer.parseInt(prop
							.getProperty("hsql_connectionpool_size")));

		}
		LOG.info("ResourceService init() completed.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.bluejungle.pf.domain.destiny.serviceprovider.IResourceAttributeProvider
	 * #getAttribute(com.bluejungle.pf.domain.epicenter.resource.IResource,
	 * java.lang.String)
	 */
	@Override
	public IEvalValue getAttribute(IResource arg0, String arg1)
			throws ServiceProviderException {

		IEvalValue value = IEvalValue.NULL;
		String resId = (String) arg0.getIdentifier();
		if (LOG.isDebugEnabled())
			LOG.info("PWC RAP arg0 identifier:" + arg0.getIdentifier());
		ArrayList<String> inputList = new ArrayList<String>();
		inputList.add(resId);
		if ("licenses".equalsIgnoreCase(arg1)) {
			value = getLicenseNos(inputList, false, LICENSEARGUMENT);
		} else if ("validlicenses".equalsIgnoreCase(arg1)) {
			value = getLicenseNos(inputList, true, LICENSEARGUMENT);
		} else if ("internallicensenos".equalsIgnoreCase(arg1)) {
			value = getLicenseNos(inputList, false, INTERNALLICENSEARGUMENT);
		} else if ("validinternallicensenos".equalsIgnoreCase(arg1)) {
			value = getLicenseNos(inputList, true, INTERNALLICENSEARGUMENT);
		}
		LOG.info(arg1 + " :" + value.toString());
		return value;
	}

	/*
	 * Get the LicenseNos(authorityIds) based on the predicates passed by the
	 * policy. if valid flag is true returns only active licenses else returns
	 * both active and inactive licenses. This single method returns the list of
	 * internal licensenos and authority ids based on the coloumnargument.
	 */
	private IEvalValue getLicenseNos(ArrayList<String> sArrDataInput,
			boolean validFlag, String colArgument) {

		LOG.info("ResourceService getLicenseNos() called.");
		LOG.debug("ResourceService getLicenseNos() sColumn.length : "
				+ sArrDataInput.size());
		IEvalValue evalue = emptySet;
		if (sArrDataInput.size() < 1) {
			LOG.warn("Wrong number of parameters.");
			return evalue;
		}

		LOG.debug("ResourceService sArrDataInput : " + sArrDataInput);
		LOG.debug("ResourceService sArrDataInput.size() : "
				+ sArrDataInput.size());
		ArrayList<String> licenses = hsqlHelper
				.retrieveLicenses(prepareQueryforLicenses(sArrDataInput,
						colArgument, validFlag));
		LOG.debug(" ResourceService Licenses " + licenses);
		if (licenses != null) {
			evalue = prepareLicenses(licenses);
		}
		return evalue;
	}

	/**
	 * This method converts a list of licenses to the data format[IEvalValue]
	 * that can be evaluated by policy evaluation engine]
	 * 
	 * @param licenses
	 * @return
	 */
	private IEvalValue prepareLicenses(ArrayList<String> licenses) {

		IEvalValue result = null;

		if (licenses != null && licenses.size() > 0) {

			IMultivalue imv = Multivalue.create(licenses, ValueType.STRING);

			result = EvalValue.build(imv);

		} else {
			return emptySet;
		}
		return result;

	}

	/*
	 * This method prepares a parameterized query string and list of parameters
	 * into a single QueryBuilder object for the query
	 */
	private QueryBuilder prepareQueryforLicenses(
			ArrayList<String> sArrDataInput, String projColumn,
			boolean validFlag) {
		LOG.info("Resource Service prepareQueryforLicenses() called.");
		setLicenseStatus();
		QueryBuilder qb = new QueryBuilder();
		StringBuilder query = new StringBuilder(
				"SELECT {0} FROM {1} {2}  LEFT OUTER JOIN {3} {4} ON {2}.{5}={4}.{6}  WHERE LCASE(TRIM({4}.{7}))=LCASE(TRIM(?))");
		if (validFlag) {
			query.append(" AND  LCASE(TRIM({8})) {12} {11} AND   {9} < current_date AND  {10} > current_date   ");
		}

		Object[] tcArgs = { projColumn, prop.getProperty("table_au"),
				prop.getProperty("alias_au"), prop.getProperty("table_arm"),
				prop.getProperty("alias_arm"),
				prop.getProperty("au_col_AuthorityId"),
				prop.getProperty("arm_col_AuthorityId"),
				prop.getProperty("arm_col_Resource"),
				prop.getProperty("au_col_LicenseStatus"),
				prop.getProperty("au_col_StartDate"),
				prop.getProperty("au_col_EndDate"), LICENSESTATUS,
				LICENSESQLOPERATOR };
		Iterator<String> iterator = sArrDataInput.iterator();
		while (iterator.hasNext()) {
			qb.getQueryParameters().add(iterator.next());
		}
		qb.setPreparedQuery(MessageFormat.format(query.toString(), tcArgs));
		LOG.info(qb.getPreparedQuery() + " " + qb.getQueryParameters());
		return qb;

	}

}
