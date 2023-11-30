package org.apache.paimon.spark.procedure;

import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Remove orphan files procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.remove_orphan_files(table => 'tableId', [older_then => '2023-10-31 12:00:00'])
 * </code></pre>
 */
public class RemoveOrphanFilesProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("older_then", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    private RemoveOrphanFilesProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String olderThan = args.isNullAt(1) ? null : args.getString(1);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(table instanceof FileStoreTable);
                    OrphanFilesClean orphanFilesClean =
                            new OrphanFilesClean((FileStoreTable) table);
                    if (!StringUtils.isBlank(olderThan)) {
                        orphanFilesClean.olderThan(olderThan);
                    }
                    try {
                        int deleted = orphanFilesClean.clean();
                        return new InternalRow[] {
                            newInternalRow(UTF8String.fromString("Deleted=" + deleted))
                        };
                    } catch (Exception e) {
                        throw new RuntimeException("Call remove_orphan_files error", e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RemoveOrphanFilesProcedure>() {
            @Override
            public RemoveOrphanFilesProcedure doBuild() {
                return new RemoveOrphanFilesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RemoveOrphanFilesProcedure";
    }
}
