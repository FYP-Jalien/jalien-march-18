package alien.taskQueue;

public class Constraint {

    private String name;
    private String expression;
    private boolean isEnabled;

    public Constraint(String name, String expression, boolean isEnabled) {
        this.name = name;
        this.expression = expression;
        this.isEnabled = isEnabled;
    }

    public String getName() {

        return name;
    }

    public String getExpression() {

        return expression;
    }

    public boolean isEnabled() {

        return isEnabled;
    }
}
