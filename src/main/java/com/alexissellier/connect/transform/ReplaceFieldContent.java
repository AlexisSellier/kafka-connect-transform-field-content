/**
 * Copyrigh0t © 2017 Alexis Sellier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alexissellier.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class ReplaceFieldContent<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Rename fields content";
    
    interface ConfigName {
	String FIELDNAME = "fieldname";
	String RENAME = "renames";
    }
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
	.define(ConfigName.FIELDNAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
		"Field to perfomace value replacement ")
	.define(ConfigName.RENAME, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
                @Override
                public void ensureValid(String name, Object value) {
                    parseRenameMappings((List<String>) value);
                }
		
                @Override
                public String toString() {
                    return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                }
            }, ConfigDef.Importance.MEDIUM, "Field content rename mappings.");

    private static final String PURPOSE = "field content replacement";

    private String fieldname;
    private Map<String, String> renames;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldname = config.getString(ConfigName.FIELDNAME); 
        this.renames = parseRenameMappings(config.getList(ConfigName.RENAME));
    }

    static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ConfigName.RENAME, mappings, "Invalid rename mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }


    String renamed(String value) {
        final String mapping = renames.get(value);
        return mapping == null ? value : mapping;
    }

    @Override
    public R apply(R record) {
	Map<String, Object> value = requireMap(record.value(), PURPOSE);
	Object e = value.get(this.fieldname);
	boolean exists = false;
	if (e != null) {
	    exists = true;
	    if (e  instanceof java.lang.Long) {
		value.put(this.fieldname, this.renamed(String.valueOf(e)));   
	    } else {
		value.put(this.fieldname, this.renamed((String) e));
	    }
	}
	return exists ? record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, value, record.timestamp()) : record;
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
	
    }

}
