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
package org.apache.ranger.authorization.presto.authorizer;

import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;

import java.util.Map;

public class RangerSystemAccessControlFactory
        implements SystemAccessControlFactory {
    private static final String NAME = "ranger";


    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public SystemAccessControl create(Map<String, String> config) {
//        RangerConfiguration rangerConfig = RangerConfiguration.getInstance();
//        try {
//            handleKerberos(rangerConfig, config);
//        }
//        catch (IOException e) {
//            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to do kerberos right", e);
//        }
//
//        for (final Map.Entry<String, String> configEntry : config.entrySet()) {
//            if (configEntry.getKey().startsWith("ranger.")) {
//                rangerConfig.set(configEntry.getKey(), configEntry.getValue());
//                LOG.info("Setting: " + configEntry.getKey() + " to: " + configEntry.getValue());
//            }
//        }
//
//        PrestoAuthorizer authorizer = getPrestoAuthorizer(config);
//        requireNonNull(config, "config is null");
//        checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
        return new RangerSystemAccessControl(config);
    }
}
