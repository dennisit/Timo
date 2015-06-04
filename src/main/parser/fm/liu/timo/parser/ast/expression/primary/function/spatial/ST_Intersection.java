package fm.liu.timo.parser.ast.expression.primary.function.spatial;

import java.util.List;

import fm.liu.timo.parser.ast.expression.Expression;
import fm.liu.timo.parser.ast.expression.primary.function.FunctionExpression;
import fm.liu.timo.parser.visitor.Visitor;

public class ST_Intersection extends FunctionExpression {

    public ST_Intersection(List<Expression> arguments) {
        super("ST_INTERSECTION", arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new ST_Intersection(arguments);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

}
