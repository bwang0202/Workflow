package workflow;

public interface CommonDefs {
	/**
	 * The file name for storing task 1's result
	 */
	public static final String ASCENDING_RESULT = "ascending_result";
	/**
	 * The file name for storing task 2's result
	 */
	public static final String DESCENDING_RESULT = "descending_result";
	/**
	 * The file name for storing task 3's result
	 */
	public static final String DEDUPLICATION_RESULT = "deduplication_result";
	/**
	 * The file name for storing task 4's result
	 */
	public static final String MULTIPLY_RESULT = "multiply_result";
	/**
	 * The file name for storing task 5's result
	 */
	public static final String DIVIDE_RESULT = "divide_result";
	/**
	 * prefix for temporary files used for storing ascending sorted subarray
	 */
	public static final String ASC_TEMP_PREFIX = "_ascending";
	/**
	 * prefix for temporary files used for storing descending sorted subarray
	 */
	public static final String DESC_TEMP_PREFIX = "_descending";
	/**
	 * Subarray size
	 */
	public static final int SUBARRAY_SIZE = 1000000;
}
