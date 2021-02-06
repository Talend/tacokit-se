/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.assertion.service;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.talend.components.assertion.conf.Config.AssertEntry;
import org.talend.components.assertion.conf.Config.Condition;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordVisitor;

public class RecordToAssertionVisitor implements RecordVisitor<List<AssertEntry>> {

    final List<AssertEntry> assertions = new ArrayList<>();

    final String path;

    public RecordToAssertionVisitor() {
        this("/");
    }

    public RecordToAssertionVisitor(String path) {
        this.path = path;
    }

    public List<AssertEntry> get() {
        return assertions;
    }

    public List<AssertEntry> apply(final List<AssertEntry> t1, final List<AssertEntry> t2) {
        // t2.stream().forEach(e -> t1.add(e));
        // t1.addAll(t2);
        return t1;
    }

    private void addAssertion(final Entry entry, String value) {
        final String s = this.path + entry.getName();
        assertions.add(new AssertEntry(s, entry.getType(), entry.getType(), Condition.EQUALS, value, "",
                s + " should be equals to '" + value + "'."));
    }

    public void onInt(final Entry entry, final OptionalInt optionalInt) {
        this.addAssertion(entry, String.valueOf(optionalInt.getAsInt()));
    }

    public void onLong(final Entry entry, final OptionalLong optionalLong) {
        this.addAssertion(entry, String.valueOf(optionalLong.getAsLong()));
    }

    public void onFloat(final Entry entry, final OptionalDouble optionalFloat) {
        this.addAssertion(entry, String.valueOf(optionalFloat.getAsDouble()));
    }

    public void onDouble(final Entry entry, final OptionalDouble optionalDouble) {
        this.addAssertion(entry, String.valueOf(optionalDouble.getAsDouble()));
    }

    public void onBoolean(final Entry entry, final Optional<Boolean> optionalBoolean) {
        this.addAssertion(entry, String.valueOf(optionalBoolean.get()));
    }

    public void onString(final Entry entry, final Optional<String> string) {
        this.addAssertion(entry, String.valueOf(string.get()));
    }

    public void onDatetime(final Entry entry, final Optional<ZonedDateTime> dateTime) {
        this.addAssertion(entry, String.valueOf(dateTime.get()));
    }

    public void onBytes(final Entry entry, final Optional<byte[]> bytes) {
        this.addAssertion(entry, Util.byteArrayToString(bytes.get()));
    }

    public RecordVisitor<List<AssertEntry>> onRecord(final Entry entry, final Optional<Record> record) {
        return new RecordToAssertionVisitor(this.path + entry.getName() + "/");
    }

    public void onIntArray(final Entry entry, final Optional<Collection<Integer>> array) {
        final Collection<Integer> integers = array.get();
        final Optional<Integer> first = integers.stream().findFirst();
        if (first.isPresent()) {
            this.onInt(entry, OptionalInt.of(first.get()));
        }
    }

    public void onLongArray(final Entry entry, final Optional<Collection<Long>> array) {
        final Collection<Long> longs = array.get();
        final Optional<Long> first = longs.stream().findFirst();
        if (first.isPresent()) {
            this.onLong(entry, OptionalLong.of(first.get()));
        }
    }

    public void onFloatArray(final Entry entry, final Optional<Collection<Float>> array) {
        final Collection<Float> floats = array.get();
        final Optional<Float> first = floats.stream().findFirst();
        if (first.isPresent()) {
            this.onFloat(entry, OptionalDouble.of((double) first.get()));
        }
    }

    public void onDoubleArray(final Entry entry, final Optional<Collection<Double>> array) {
        final Collection<Double> doubles = array.get();
        final Optional<Double> first = doubles.stream().findFirst();
        if (first.isPresent()) {
            this.onDouble(entry, OptionalDouble.of(first.get()));
        }
    }

    public void onBooleanArray(final Entry entry, final Optional<Collection<Boolean>> array) {
        final Collection<Boolean> booleans = array.get();
        final Optional<Boolean> first = booleans.stream().findFirst();
        if (first.isPresent()) {
            this.onBoolean(entry, Optional.of(first.get()));
        }
    }

    public void onStringArray(final Entry entry, final Optional<Collection<String>> array) {
        final Collection<String> Strings = array.get();
        final Optional<String> first = Strings.stream().findFirst();
        if (first.isPresent()) {
            this.onString(entry, Optional.of(first.get()));
        }
    }

    public void onDatetimeArray(final Entry entry, final Optional<Collection<ZonedDateTime>> array) {
        final Collection<ZonedDateTime> zdts = array.get();
        final Optional<ZonedDateTime> first = zdts.stream().findFirst();
        if (first.isPresent()) {
            this.onDatetime(entry, Optional.of(first.get()));
        }
    }

    public void onBytesArray(final Entry entry, final Optional<Collection<byte[]>> array) {
        final Collection<byte[]> bytes = array.get();
        final Optional<byte[]> first = bytes.stream().findFirst();
        if (first.isPresent()) {
            this.onBytes(entry, Optional.of(first.get()));
        }
    }

    public RecordVisitor<List<AssertEntry>> onRecordArray(final Entry entry, final Optional<Collection<Record>> array) {
        final Collection<Record> records = array.get();
        final Optional<Record> first = records.stream().findFirst();
        if (first.isPresent()) {
            return this.onRecord(entry, Optional.of(first.get()));
        } else {
            throw new RuntimeException("Array of record should not be empty when generating configuration.");
        }
    }

}
