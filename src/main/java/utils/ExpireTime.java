package utils;

import java.io.Serializable;

/**
 * @author ibrinzoi
 * @since 2021-01-19
 */
public class ExpireTime implements Serializable {
	private int days;
	private int weeks;
	private int months;
	private int years;

	private static final long serialVersionUID = -8078265238403408325L;

	/**
	 * Default constructor, zero expiration time
	 */
	public ExpireTime() {
		this.days = 0;
		this.weeks = 0;
		this.months = 0;
		this.years = 0;
	}

	/**
	 * @param days number of days until the file expires, or to extend the expiration with
	 */
	public void setDays(final int days) {
		this.days = days;
	}

	/**
	 * @param weeks number of weeks until the file expires, or to extend the expiration with
	 */
	public void setWeeks(final int weeks) {
		this.weeks = weeks;
	}

	/**
	 * @param months number of months until the file expires, or to extend the expiration with
	 */
	public void setMonths(final int months) {
		this.months = months;
	}

	/**
	 * @param years number of years until the file expires, or to extend the expiration with
	 */
	public void setYears(final int years) {
		this.years = years;
	}

	/**
	 * @return number of days until expiration (independent of the other fields)
	 */
	public int getDays() {
		return this.days;
	}

	/**
	 * @return number of weeks until expiration (independent of the other fields)
	 */
	public int getWeeks() {
		return this.weeks;
	}

	/**
	 * @return number of months until expiration (independent of the other fields)
	 */
	public int getMonths() {
		return this.months;
	}

	/**
	 * @return number of years until expiration (independent of the other fields)
	 */
	public int getYears() {
		return this.years;
	}
}