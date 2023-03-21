package org.apache.paimon.format;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestFileFormatFactory implements FileFormatFactory {

    @Override
    public String identifier() {
        return "test";
    }

    @Override
    public FileFormat create(FormatContext context) {
        assertThat(context.formatOptions().get("hello")).isEqualTo("world");
        assertThat(context.readBatchSize()).isEqualTo(1024);
        throw new SuccessException();
    }

    public static class SuccessException extends RuntimeException {}

    @Test
    public void testCreateFileFormat() {
        Options options = new Options();
        options.setString("hello", "world");
        assertThrows(
                SuccessException.class,
                () -> new TestFileFormatFactory().create(new FormatContext(options, 1024)));
    }
}
