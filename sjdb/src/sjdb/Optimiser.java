package sjdb;

import java.util.*;
import java.util.stream.Collectors;


public class Optimiser implements PlanVisitor {

    private Catalogue cat;

    private static final Estimator EST = new Estimator(); // Using the Estimator here

    public Optimiser(Catalogue cat) {
		this.cat = cat;
	}

    public Operator optimise(Operator plan) {
        // Visit the plan to populate sets
        plan.accept(this);

        // Push SELECTs and PROJECTs down to SCAN leaves
        List<Operator> operationBlocks = pushSelectsAndProjectsDownForScans(allScans, allPredicates, plan);

        // Create the best order of JOINs or PRODUCTS
        return createBestOrderOfJoinOrProducts(allPredicates, operationBlocks, plan);
    }

    private Operator createBestOrderOfJoinOrProducts(Set<Predicate> originalPreds, List<Operator> ops, Operator root){
        List<Predicate> preds = new ArrayList<>(originalPreds);
        List<List<Predicate>> permutedPreds = generatePermutations(preds);

        Operator cheapestPlan = null;
        int cheapestCost = Integer.MAX_VALUE;

        for (List<Predicate> p : permutedPreds) {
            List<Operator> tempOps = new ArrayList<>(ops);
            Operator aPlan = buildProductOrJoin(tempOps, p, root);
            int cost = EST.getCost(aPlan);
            System.out.println("Found plan with cost: " + cost);
            if (cost < cheapestCost) {
                cheapestPlan = aPlan;
                cheapestCost = cost;
            }
        }
        return cheapestPlan;
    }

    private List<Operator> pushSelectsAndProjectsDownForScans(Set<Scan> scans, Set<Predicate> predicates, Operator root) {
        List<Operator> operatorBlocks = new ArrayList<>();

        for (Scan s : scans) {
            Operator o = buildSelectsOnTop(s, predicates);
            List<Predicate> temp = new ArrayList<>(predicates);
            getNecessaryAttrs(temp, root);
            operatorBlocks.add(buildProjectOnTop(o, getNecessaryAttrs(temp, root)));
        }

        return operatorBlocks;
    }

    private Operator buildSelectsOnTop(Operator op, Set<Predicate> preds) {
        Operator result = op;
        List<Attribute> availableAttrs = result.getOutput().getAttributes();

        Iterator<Predicate> it = preds.iterator();
        while (it.hasNext()) {
            Predicate currentPred = it.next();
            if (result.getOutput() == null) result.accept(EST);
            if ((currentPred.equalsValue() && availableAttrs.contains(currentPred.getLeftAttribute())) ||
                (!currentPred.equalsValue() && availableAttrs.contains(currentPred.getLeftAttribute()) && availableAttrs.contains(currentPred.getRightAttribute()))) {
                result = new Select(result, currentPred);
                it.remove();
            }
        }

        return result;
    }

    private Operator buildProjectOnTop(Operator op, Set<Attribute> attrs) {
        if (op.getOutput() == null) op.accept(EST);
        List<Attribute> attrsToProjectFromOp = new ArrayList<>(attrs);
        attrsToProjectFromOp.retainAll(op.getOutput().getAttributes());

        if (attrsToProjectFromOp.size() > 0) {
            Operator op2 = new Project(op, attrsToProjectFromOp);
            op2.accept(EST);
            return op2;
        } else {
            return op;
        }
    }

    private Operator buildProductOrJoin(List<Operator> ops, List<Predicate> preds, Operator root) {
        if (ops.size() == 1) {
            Operator result = ops.get(0);
            if (result.getOutput() == null) result.accept(EST);
            return result;
        }

        Iterator<Predicate> it = preds.iterator();
        while (it.hasNext()) {
            Predicate currentPred = it.next();
            Operator left = extractOperatorForAttribute(ops, currentPred.getLeftAttribute());
            Operator right = extractOperatorForAttribute(ops, currentPred.getRightAttribute());
            Operator result = null;


            if ((left == null && right != null) || (right == null && left != null)) {
                result = new Select(left != null ? left : right, currentPred);
                it.remove();
            }

            if (left != null && right != null) {
                result = new Join(left, right, currentPred);
                it.remove();
            }

            if (result.getOutput() == null) result.accept(EST);
            Set<Attribute> neededAttrs = getNecessaryAttrs(preds, root);
            List<Attribute> availableAttrs = result.getOutput().getAttributes();

            if (neededAttrs.size() == availableAttrs.size() && availableAttrs.containsAll(neededAttrs)) {
                ops.add(result);
            } else {
                List<Attribute> attrsToKeep = availableAttrs.stream().filter(attr -> neededAttrs.contains(attr)).collect(Collectors.toList());
                if (attrsToKeep.size() == 0) {
                    ops.add(result);
                } else {
                    Project tempProj = new Project(result, attrsToKeep);
                    tempProj.accept(EST);
                    ops.add(tempProj);
                }
            }
        }

        while (ops.size() > 1) {
            Operator b1 = ops.get(0);
            Operator b2 = ops.get(1);
            Operator product = new Product(b1, b2);
            product.accept(EST);
            ops.remove(b1);
            ops.remove(b2);
            ops.add(product);
        }

        return ops.get(0);
    }

    private Operator extractOperatorForAttribute(List<Operator> oList, Attribute attr) {
        Iterator<Operator> oIt = oList.iterator();
        while (oIt.hasNext()) {
            Operator curOp = oIt.next();
            if (curOp.getOutput().getAttributes().contains(attr)) {
                oIt.remove();
                return curOp;
            }
        }
        return null;
    }

    private Set<Attribute> getNecessaryAttrs(List<Predicate> predicates, Operator root) {
        Set<Attribute> attrsNeeded = new HashSet<>();
        Iterator<Predicate> predIt = predicates.iterator();
        while (predIt.hasNext()) {
            Predicate currentPred = predIt.next();
            Attribute left = currentPred.getLeftAttribute();
            Attribute right = currentPred.getRightAttribute();
            attrsNeeded.add(left);
            if (right != null) attrsNeeded.add(right);
        }
        if (root instanceof Project) attrsNeeded.addAll(((Project) root).getAttributes());
        return attrsNeeded;
    }

    private List<List<Predicate>> generatePermutations(List<Predicate> preds) {
        if (preds.size() == 0) {
            List<List<Predicate>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }

        Predicate first = preds.remove(0);
        List<List<Predicate>> returnValue = new ArrayList<>();
        List<List<Predicate>> permutations = generatePermutations(preds);

        for (List<Predicate> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<Predicate> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, first);
                returnValue.add(temp);
            }
        }

        return returnValue;
    }

    private Set<Attribute> allAttributes = new HashSet<>();
    private Set<Predicate> allPredicates = new HashSet<>();
    private Set<Scan> allScans = new HashSet<>();

    public void visit(Scan op) { allScans.add(new Scan((NamedRelation) op.getRelation())); }
    public void visit(Project op) { allAttributes.addAll(op.getAttributes()); }
    public void visit(Product op) {}
    public void visit(Join op) {}
    public void visit(Select op) {
        allPredicates.add(op.getPredicate());
        allAttributes.add(op.getPredicate().getLeftAttribute());
        if (!op.getPredicate().equalsValue()) allAttributes.add(op.getPredicate().getRightAttribute());
    }
}
