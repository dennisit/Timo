package fm.liu.timo.parser.ast.expression.primary.function.spatial;

import java.util.List;
import fm.liu.timo.parser.ast.expression.Expression;
import fm.liu.timo.parser.ast.expression.primary.function.FunctionExpression;
import fm.liu.timo.parser.visitor.Visitor;

public class Centroid extends FunctionExpression {

    public Centroid(Expression expr) {
        super("CENTROID", wrapList(expr));
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        throw new UnsupportedOperationException("function of char has special arguments");
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
}
