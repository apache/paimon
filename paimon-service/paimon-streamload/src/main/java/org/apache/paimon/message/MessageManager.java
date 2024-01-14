/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.message;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/** sdsd. */
public class MessageManager {

    private static final Logger LOG = LoggerFactory.getLogger(MessageManager.class);

    private Configuration config = createTemplateConfiguration();

    private Configuration createTemplateConfiguration() {
        Map<String, String> responseMessages = loadResponseMessages();
        StringTemplateLoader templateLoader = new StringTemplateLoader();
        for (Entry<String, String> entry : responseMessages.entrySet()) {
            templateLoader.putTemplate(entry.getKey(), entry.getValue());
        }
        Configuration config = new Configuration(Configuration.VERSION_2_3_28);
        config.setTemplateLoader(templateLoader);
        config.setDefaultEncoding("UTF-8");
        config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        return config;
    }

    private Map<String, String> loadResponseMessages() {
        Map<String, String> messages = new HashMap<>();
        try (InputStream in =
                getClass().getClassLoader().getResourceAsStream("response-message.properties")) {
            Properties properties = new Properties();
            properties.load(in);
            for (Entry<Object, Object> entry : properties.entrySet()) {
                messages.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
            return messages;
        } catch (IOException e) {
            LOG.error("failed to load response messages", e);
            throw new RuntimeException(e);
        }
    }

    public String render(String key, Object model) {
        StringWriter writer = new StringWriter();
        try {
            config.getTemplate(key).process(model, writer);
        } catch (TemplateException | IOException e) {
            LOG.error("failed to render template[{}] with model[{}]", key, model);
        }
        return writer.toString();
    }
}
