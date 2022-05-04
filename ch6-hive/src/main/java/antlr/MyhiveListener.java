// Generated from Myhive.g4 by ANTLR 4.9.3
package antlr;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link MyhiveParser}.
 */
public interface MyhiveListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link MyhiveParser#select_stmt_1}.
	 * @param ctx the parse tree
	 */
	void enterSelect_stmt_1(MyhiveParser.Select_stmt_1Context ctx);
	/**
	 * Exit a parse tree produced by {@link MyhiveParser#select_stmt_1}.
	 * @param ctx the parse tree
	 */
	void exitSelect_stmt_1(MyhiveParser.Select_stmt_1Context ctx);
	/**
	 * Enter a parse tree produced by {@link MyhiveParser#select_stmt_2}.
	 * @param ctx the parse tree
	 */
	void enterSelect_stmt_2(MyhiveParser.Select_stmt_2Context ctx);
	/**
	 * Exit a parse tree produced by {@link MyhiveParser#select_stmt_2}.
	 * @param ctx the parse tree
	 */
	void exitSelect_stmt_2(MyhiveParser.Select_stmt_2Context ctx);
	/**
	 * Enter a parse tree produced by {@link MyhiveParser#col_name}.
	 * @param ctx the parse tree
	 */
	void enterCol_name(MyhiveParser.Col_nameContext ctx);
	/**
	 * Exit a parse tree produced by {@link MyhiveParser#col_name}.
	 * @param ctx the parse tree
	 */
	void exitCol_name(MyhiveParser.Col_nameContext ctx);
}