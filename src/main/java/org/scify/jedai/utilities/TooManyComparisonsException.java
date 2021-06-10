package org.scify.jedai.utilities;

import static org.scify.jedai.utilities.IConstants.MAX_COMPARISONS;
/**
* Signals that the number of comparisons is excessively high. The maximum number of comparisons
* allowed is specified by {@link IConstants#MAX_COMPARISONS}.
*/

public class TooManyComparisonsException extends RuntimeException {
	private static final long serialVersionUID = -4822529736751878936L;

	/**
     * Constructs a new {@code TooManyComparisonsException} with an internally generated message as
     * its detail message. The cause is not initialized, and may subsequently be initialized by a
    * call to {@link #initCause}.
     * 
     * @param comparisonCount the number of requested comparisons. This number is expected to be
     *     greater than {@link IConstants#MAX_COMPARISONS}.
     */
    public TooManyComparisonsException(long comparisonCount) {
        super(formatMessage(comparisonCount));
    }

    /**
     * Formats a string to be used as the detail message of the exception.
     */
    private static String formatMessage(long comparisonCount) {
      return String.format("Requested number of comparisons [%,d] exceeds the maximum allowed "
        + "limit [%,d].", comparisonCount, MAX_COMPARISONS);
    }
}
