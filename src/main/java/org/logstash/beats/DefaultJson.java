package org.logstash.beats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

class DefaultJson {

    private static final ObjectMapper mapper = new ObjectMapper(new JsonFactory()).registerModule(new AfterburnerModule());
    private static final ObjectReader reader = mapper.reader();
    static ObjectReader get() {
        return reader;
    }
    static ObjectWriter getWriterInstance() {
        return mapper.writer();
    }

    private DefaultJson() {
        
    }

}
