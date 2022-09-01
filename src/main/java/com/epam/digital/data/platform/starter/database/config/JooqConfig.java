/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.starter.database.config;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import javax.sql.DataSource;

import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalTimeDeserializer;
import org.jooq.Converter;
import org.jooq.ConverterProvider;
import org.jooq.DSLContext;
import org.jooq.JSON;
import org.jooq.SQLDialect;
import org.jooq.exception.DataTypeException;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultConverterProvider;
import org.jooq.impl.EnumConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

@ConditionalOnClass(DSLContext.class)
@Configuration
public class JooqConfig {

  @Bean
  public DataSourceConnectionProvider connectionProvider(DataSource dataSource) {
    return new DataSourceConnectionProvider
        (new TransactionAwareDataSourceProxy(dataSource));
  }

  @Bean
  public ConverterProvider converterProvider(@Qualifier("jooqMapper") ObjectMapper jooqMapper) {
    return new ConverterProvider() {
      final ConverterProvider defaultConverterProvider = new DefaultConverterProvider();

      @Override
      public <T, U> Converter<T, U> provide(Class<T> tType, Class<U> uType) {
        if (uType == LocalDate.class) {
          return (Converter<T, U>)
              Converter.ofNullable(Date.class, LocalDate.class, Date::toLocalDate, Date::valueOf);
        } else if (uType == LocalTime.class) {
          return (Converter<T, U>)
              Converter.ofNullable(Time.class, LocalTime.class, Time::toLocalTime, Time::valueOf);
        } else if (uType == LocalDateTime.class) {
          return (Converter<T, U>)
              Converter.ofNullable(
                  Timestamp.class,
                  LocalDateTime.class,
                  Timestamp::toLocalDateTime,
                  Timestamp::valueOf);
        } else if (Enum.class.isAssignableFrom(uType)) {
          return new EnumConverter(tType, uType);
        } else if (tType == JSON.class) {
          return Converter.ofNullable(
              tType,
              uType,
              t -> {
                try {
                  return jooqMapper.readValue(((JSON) t).data(), uType);
                } catch (Exception var4) {
                  throw new DataTypeException("JSON mapping error", var4);
                }
              },
              u -> {
                try {
                  return (T) JSON.valueOf(jooqMapper.writeValueAsString(u));
                } catch (Exception var3) {
                  throw new DataTypeException("JSON mapping error", var3);
                }
              });
        } else {
          return defaultConverterProvider.provide(tType, uType);
        }
      }
    };
  }

  @Bean("jooqMapper")
  public ObjectMapper jooqMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    mapper.setDateFormat((new StdDateFormat()).withColonInTimeZone(true));
    JavaTimeModule module = new JavaTimeModule();
    module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
    module.addDeserializer(LocalTime.class, new LocalTimeDeserializer(DateTimeFormatter.ISO_LOCAL_TIME));
    module.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ISO_LOCAL_DATE));
    mapper.registerModule(module);
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.disable(MapperFeature.USE_ANNOTATIONS);
    return mapper;
  }

  @Bean
  public DefaultConfiguration configuration(DataSourceConnectionProvider connectionProvider,
      ConverterProvider converterProvider) {
    return (DefaultConfiguration) new DefaultConfiguration()
        .derive(connectionProvider)
        .derive(SQLDialect.POSTGRES)
        .set(converterProvider);
  }

}
