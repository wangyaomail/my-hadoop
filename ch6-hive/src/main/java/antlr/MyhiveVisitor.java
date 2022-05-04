// Generated from Myhive.g4 by ANTLR 4.9.3
package antlr;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link MyhiveParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface MyhiveVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link MyhiveParser#select_stmt_1}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_stmt_1(MyhiveParser.Select_stmt_1Context ctx);
	/**
	 * Visit a parse tree produced by {@link MyhiveParser#select_stmt_2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect_stmt_2(MyhiveParser.Select_stmt_2Context ctx);
	/**
	 * Visit a parse tree produced by {@link MyhiveParser#col_name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol_name(MyhiveParser.Col_nameContext ctx);
}