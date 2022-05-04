package antlr;


import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Scanner;

public class RunMyHive {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true){
            String scanText = scanner.nextLine();
            CharStream stream = CharStreams.fromString(scanText);
            MyhiveLexer lexer = new MyhiveLexer(stream);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MyhiveParser parser = new MyhiveParser(tokens);
            ParseTree tree = parser.select_stmt_2();
            System.out.println(tree.toStringTree());
            MyhiveVisitorImpl visitor = new MyhiveVisitorImpl();
            String result = visitor.visit(tree);
            System.out.println(result);
        }

    }
}
