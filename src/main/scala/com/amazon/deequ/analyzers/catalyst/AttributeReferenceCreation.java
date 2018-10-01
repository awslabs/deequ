package com.amazon.deequ.analyzers.catalyst;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeReference$;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression$;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.Metadata$;

import java.lang.reflect.Method;

public class AttributeReferenceCreation {

    private AttributeReferenceCreation() { }

    /**
        Allows us to invoke the apply method on
        org.apache.spark.sql.catalyst.expressions.AttributeReference which has a non-compatible
        signature in Spark 2.x. Therefore we need to invoke it via reflection depending on which
        version of Spark we run.

        SPARK 2.3:

            case class AttributeReference(
                name: String,
                dataType: DataType,
                nullable: Boolean = true,
                override val metadata: Metadata = Metadata.empty)(
                val exprId: ExprId = NamedExpression.newExprId,
                val qualifier: Option[String] = None)

        SPARK 2.2:

            case class AttributeReference(
                name: String,
                dataType: DataType,
                nullable: Boolean = true,
                override val metadata: Metadata = Metadata.empty)(
                val exprId: ExprId = NamedExpression.newExprId,
                val qualifier: Option[String] = None,
                override val isGenerated: java.lang.Boolean = false)
    */
    public static AttributeReference createSafe(String name) throws IllegalStateException {
        try {

            Class clazz = AttributeReference$.class;
            Method apply = null;

            for (Method method : clazz.getMethods()) {
                if (method.getName().equals("apply")) {
                    apply = method;
                    break;
                }
            }

            LongType longType = LongType$.MODULE$.asNullable();
            Metadata emptyMetadata = Metadata$.MODULE$.empty();
            scala.Option none = scala.Option.apply(null);
            ExprId exprId = NamedExpression$.MODULE$.newExprId();

            Object singleton = AttributeReference$.MODULE$;

            if (apply.getParameterCount() == 7) {
                return (AttributeReference) apply.invoke(singleton, name, longType, true,
                        emptyMetadata, exprId, none, false);
            } else {
                return (AttributeReference) apply.invoke(singleton, name, longType, true,
                        emptyMetadata, exprId, none);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
