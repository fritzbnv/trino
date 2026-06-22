/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.spool;

import io.airlift.slice.Slice;
import io.trino.spi.spool.SpooledLocation.DirectLocation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SpoolingManager
{
    SpooledSegmentHandle create(SpoolingContext context);

    OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException;

    InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException;

    void acknowledge(SpooledSegmentHandle handle)
            throws IOException;

    Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException;

    // Converts the handle to a location that client will be redirected to
    SpooledLocation location(SpooledSegmentHandle handle)
            throws IOException;

    // Converts spooled location back to the handle
    SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers);

    /**
     * Determines whether an exception thrown while spooling a segment is recoverable.
     * <p>
     * The engine may fall back to inlining a segment that failed to spool, but only for recoverable
     * (transient) failures, such as a temporarily unavailable storage backend. Unrecoverable failures
     * (for example invalid credentials or a misconfigured location) are not masked this way: the query
     * fails instead, so that the underlying problem is surfaced rather than silently degrading to
     * inlining on every segment.
     * <p>
     * The default implementation treats every failure as unrecoverable; implementations that can
     * classify their failures should override this to opt into the inlining fallback for transient
     * errors.
     *
     * @return {@code true} if the failure is transient and inlining the segment is a reasonable fallback
     */
    default boolean isRecoverableException(IOException exception)
    {
        return false;
    }
}
