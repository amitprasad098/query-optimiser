package sjdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Estimator implements PlanVisitor {
	
	private int totalCost;


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
        Relation input = op.getRelation();
        Relation output = createOutputRelation(input);
        op.setOutput(output);
        totalCost += output.getTupleCount();
    }

    public void visit(Project op) {
        Relation input = op.getInput().getOutput();
        Relation output = new Relation(input.getTupleCount());
        for (Attribute attr_needed : op.getAttributes()) {
            for (Attribute attr_found : input.getAttributes()) {
                if (attr_needed.equals(attr_found)) {
                    output.addAttribute(new Attribute(attr_found.getName(), attr_found.getValueCount()));
                }
            }
        }
        op.setOutput(output);
        totalCost += output.getTupleCount();
    }
    
    public void visit(Select op) {
        Predicate p = op.getPredicate();
        Attribute attr_left = new Attribute(p.getLeftAttribute().getName());
        Attribute output_left_attr = null;
        Relation input = op.getInput().getOutput();
        Relation output;
        for (Attribute attr_found : input.getAttributes()) {
            if (attr_found.equals(attr_left)) {
                attr_left = new Attribute(attr_found.getName(), attr_found.getValueCount());
            }
        }
        if (p.equalsValue()) {
            output = createValueSelectOutput(input, attr_left);
        } else {
            output = createAttributeSelectOutput(op, input, attr_left);
        }
        op.setOutput(output);
        totalCost += output.getTupleCount();
    }
    
    public void visit(Product op) {
        Relation left = op.getLeft().getOutput();
        Relation right = op.getRight().getOutput();
        Relation output = createProductOutput(left, right);
        op.setOutput(output);
        totalCost += output.getTupleCount();
    }
    
    public void visit(Join op) {
        Relation left_rel = op.getLeft().getOutput();
        Relation right_rel = op.getRight().getOutput();
        Predicate p = op.getPredicate();
        Attribute attr_left = new Attribute(p.getLeftAttribute().getName());
        Attribute attr_right = new Attribute(p.getRightAttribute().getName());
        Relation output = createJoinOutput(left_rel, right_rel, attr_left, attr_right);
        op.setOutput(output);
        totalCost += output.getTupleCount();
    }
    
    public int getCost(Operator plan) {
        this.totalCost = 0;
        plan.accept(this);   
        return this.totalCost;
    }

    private Relation createOutputRelation(Relation input) {
        Relation output = new Relation(input.getTupleCount());
        Iterator<Attribute> iter = input.getAttributes().iterator();
        while (iter.hasNext()) {
            output.addAttribute(new Attribute(iter.next()));
        }
        return output;
    }

    private Relation createValueSelectOutput(Relation input, Attribute attr_left) {
        Relation output = new Relation(input.getTupleCount() / attr_left.getValueCount());
        Attribute output_left_attr = new Attribute(attr_left.getName(), Math.min(1, output.getTupleCount()));
        for (Attribute attr : input.getAttributes()) {
            if (!attr.equals(attr_left)) {
                output.addAttribute(new Attribute(attr));
            }
        }
        output.addAttribute(output_left_attr);
        return output;
    }

    private Relation createAttributeSelectOutput(Select op, Relation input, Attribute attr_left) {
        Attribute attr_right = new Attribute(op.getPredicate().getRightAttribute().getName());
        Relation output = new Relation(input.getTupleCount() / Math.max(attr_left.getValueCount(), attr_right.getValueCount()));
        int size = Math.min(Math.min(attr_left.getValueCount(), attr_right.getValueCount()), output.getTupleCount());
        Attribute output_left_attr = new Attribute(attr_left.getName(), size);
        Attribute output_right_attr = new Attribute(attr_right.getName(), size);
        for (Attribute attr : input.getAttributes()) {
            if (!attr.equals(attr_left) && !attr.equals(attr_right)) {
                output.addAttribute(new Attribute(attr));
            }
        }
        output.addAttribute(output_left_attr);
        output.addAttribute(output_right_attr);
        return output;
    }

    private Relation createProductOutput(Relation left, Relation right) {
        Relation output = new Relation(left.getTupleCount() * right.getTupleCount());
        left.getAttributes().forEach(attr -> output.addAttribute(new Attribute(attr.getName(), attr.getValueCount())));
        right.getAttributes().forEach(attr -> output.addAttribute(new Attribute(attr.getName(), attr.getValueCount())));
        return output;
    }

    private Relation createJoinOutput(Relation left_rel, Relation right_rel, Attribute attr_left, Attribute attr_right) {
        List<Attribute> all_attrs = new ArrayList<>();
        all_attrs.addAll(left_rel.getAttributes());
        all_attrs.addAll(right_rel.getAttributes());
        for (Attribute attr_found : all_attrs) {
            if (attr_found.equals(attr_left)) {
                attr_left = new Attribute(attr_found.getName(), attr_found.getValueCount());
            }
            if (attr_found.equals(attr_right)) {
                attr_right = new Attribute(attr_found.getName(), attr_found.getValueCount());
            }
        }
        Relation output = new Relation(left_rel.getTupleCount() * right_rel.getTupleCount() / Math.max(attr_left.getValueCount(), attr_right.getValueCount()));
        int uniq_size = Math.min(Math.min(attr_left.getValueCount(), attr_right.getValueCount()), output.getTupleCount());
        Attribute join_attr_left = new Attribute(attr_left.getName(), uniq_size);
        Attribute join_attr_right = new Attribute(attr_right.getName(), uniq_size);
        Iterator<Attribute> liter = left_rel.getAttributes().iterator();
        while (liter.hasNext()) {
            Attribute attr = liter.next();
            if (!attr.equals(attr_left)) {
                output.addAttribute(new Attribute(attr));
            } else {
                output.addAttribute(join_attr_left);
            }
        }
        Iterator<Attribute> riter = right_rel.getAttributes().iterator();
        while (riter.hasNext()) {
            Attribute attr = riter.next();
            if (!attr.equals(attr_right)) {
                output.addAttribute(new Attribute(attr));
            } else {
                output.addAttribute(join_attr_right);
            }
        }
        return output;
    }
}
    
    
