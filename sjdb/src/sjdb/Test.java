package sjdb;
 
import java.util.ArrayList;
 
public class Test {
    private Catalogue catalogue;
    
    public Test() {
    }
 
    public static void main(String[] args) throws Exception {
        Catalogue catalogue = createCatalogue();
        Inspector inspector = new Inspector();
        Estimator estimator = new Estimator();
 
        Operator plan = query(catalogue);
        System.out.println("\n Query Plan before Optimiser: " + plan.toString() + " \n");
        plan.accept(estimator);
        plan.accept(inspector);
 
        Optimiser optimiser = new Optimiser(catalogue);
        Operator planopt = optimiser.optimise(plan);
        System.out.println("\n Optimised Query Plan: " + planopt.toString() + " \n");
        planopt.accept(estimator);
        planopt.accept(inspector);
    }
    
 
    public static Catalogue createCatalogue() {
        Catalogue cat = new Catalogue();
        cat.createRelation("Person", 400);
        cat.createAttribute("Person", "persid", 400);
        cat.createAttribute("Person", "persname", 350);
        cat.createAttribute("Person", "age", 47);
        cat.createRelation("Project", 40);
        cat.createAttribute("Project", "projid", 40);
        cat.createAttribute("Project", "projname", 35);
        cat.createAttribute("Project", "dept", 5);
        cat.createRelation("Department", 5);
        cat.createAttribute("Department", "deptid", 5);
        cat.createAttribute("Department", "deptname", 5);
        cat.createAttribute("Department", "manager", 5);
        
        return cat;
    }
 
    public static Operator query(Catalogue cat) throws Exception {
        Scan person = new Scan(cat.getRelation("Person"));
        Scan project = new Scan(cat.getRelation("Project"));
        Scan department = new Scan(cat.getRelation("Department"));
 
        // Creating joins based on the WHERE conditions
        Product p1 = new Product(person, department);
        Select s1 = new Select(p1, new Predicate(new Attribute("persid"), new Attribute("manager")));
 
        Product p2 = new Product(s1, project);
        Select s2 = new Select(p2, new Predicate(new Attribute("dept"), new Attribute("deptid")));
 
        // Filtering based on the person's name
        Select s3 = new Select(s2, new Predicate(new Attribute("persname"), "Smith"));
 
        ArrayList<Attribute> atts = new ArrayList<>();
        atts.add(new Attribute("projname"));
        atts.add(new Attribute("deptname"));
 
        Project plan = new Project(s3, atts);
 
        return plan;
    }
 
}
 