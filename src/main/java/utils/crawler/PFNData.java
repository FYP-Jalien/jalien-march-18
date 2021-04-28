package utils.crawler;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;

/**
 * Models the data that is gathered during the crawling process for every PFN
 *
 * @author anegru
 */
public class PFNData {
	private Map<String, Object> data = new LinkedHashMap<>();

	/**
	 * @param guid
	 * @param seNumber
	 * @param pfn
	 * @param observedSize
	 * @param catalogueSize
	 * @param observedMD5
	 * @param catalogueMD5
	 * @param downloadDurationMillis
	 * @param xrdfsDurationMillis
	 * @param statusCode
	 * @param statusType
	 * @param statusMessage
	 * @param timestamp
	 */
	public PFNData(final String guid, final Integer seNumber, final String pfn, final Long observedSize, final Long catalogueSize, final String observedMD5, final String catalogueMD5,
			final Long downloadDurationMillis, final Long xrdfsDurationMillis, final String statusCode, final String statusType, final String statusMessage, final Long timestamp) {
		data.put("guid", guid);
		data.put("seNumber", seNumber);
		data.put("pfn", pfn);
		data.put("observedSize", observedSize);
		data.put("catalogueSize", catalogueSize);
		data.put("observedMD5", observedMD5);
		data.put("catalogueMD5", catalogueMD5);
		data.put("downloadDurationMillis", downloadDurationMillis);
		data.put("xrdfsDurationMillis", xrdfsDurationMillis);
		data.put("statusCode", statusCode);
		data.put("statusType", statusType);
		data.put("statusMessage", statusMessage);
		data.put("timestamp", timestamp);
	}

	private PFNData() {
	}

	/**
	 * Set a map holding crawling data for a PFN analysed
	 * 
	 * @param data
	 */
	private void setData(final Map<String, Object> data) {
		this.data = data;
	}

	/**
	 * @return format PFNData object as string in CSV format
	 */
	public String toCSV() {
		final Collection<Object> values = data.values();
		return values.stream().map(value -> value == null ? "null" : value.toString()).collect(Collectors.joining(","));
	}

	/**
	 * @return comma separated list of all property names extracted during crawling (eg. catalogueMD5)
	 */
	public String getCsvHeader() {
		final Set<String> keys = data.keySet();
		return String.join(",", keys);
	}

	/**
	 * @return all property values computed during crawling
	 */
	public Collection<Object> getValues() {
		return data.values();
	}

	/**
	 *
	 * @return the map that is used internally to hold crawling data
	 */
	public Map<String, Object> getData() {
		return Collections.unmodifiableMap(data);
	}

	/**
	 * @return the status code of the PFN
	 */
	public String getStatusCode() {
		try {
			return data.get("statusCode").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the SE number on which the PFN analysed resides
	 */
	public String getSeNumber() {
		try {
			return data.get("seNumber").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the status type of the PFN
	 */
	public String getStatusType() {
		try {
			return data.get("statusType").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the status message of the PFN
	 */
	public String getStatusMessage() {
		try {
			return data.get("statusMessage").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the PFN as string
	 */
	public String getPfn() {
		try {
			return data.get("pfn").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the GUID of the PFN
	 */
	public String getGuid() {
		try {
			return data.get("guid").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the size in bytes of the PFN after download
	 */
	public Long getObservedSize() {
		try {
			return Long.valueOf(this.data.get("observedSize").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the size in bytes of the PFN registered in the catalogue
	 */
	public Long getCatalogueSize() {
		try {
			return Long.valueOf(this.data.get("catalogueSize").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the MD5 of the PFN recomputed after download
	 */
	public String getObservedMD5() {
		try {
			return this.data.get("observedMD5").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the MD5 of the PFN registered in the catalogue
	 */
	public String getCatalogueMD5() {
		try {
			return this.data.get("catalogueMD5").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the total duration in milliseconds of the PFN download
	 */
	public Long getDownloadDurationMillis() {
		try {
			return Long.valueOf(this.data.get("downloadDurationMillis").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the total duration in milliseconds of the xrdfs call
	 */
	public Long getXrdfsDurationMillis() {
		try {
			return Long.valueOf(this.data.get("xrdfsDurationMillis").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return the output generation timestamp
	 */
	public Long getOutputGeneratedTimestamp() {
		try {
			return Long.valueOf(this.data.get("timestamp").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * Convert from PFNData to JSONObject
	 * 
	 * @return JSONObject
	 */
	@SuppressWarnings("unchecked")
	public JSONObject toJSON() {
		final JSONObject json = new JSONObject();
		json.putAll(data);
		return json;
	}

	/**
	 * Convert from JSONObject to PFNData
	 * 
	 * @param jsonObject
	 * @return PFNData
	 */
	public static PFNData fromJSON(final JSONObject jsonObject) {
		final PFNData pfnData = new PFNData();
		final Map<String, Object> data = new LinkedHashMap<>();

		for (final Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) jsonObject.entrySet()) {
			data.put(entry.getKey().toString(), entry.getValue());
		}

		pfnData.setData(data);

		return pfnData;
	}

	@Override
	public String toString() {
		return this.toJSON().toJSONString();
	}
}
