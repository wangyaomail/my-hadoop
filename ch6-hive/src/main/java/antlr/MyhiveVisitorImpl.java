package antlr;

public class MyhiveVisitorImpl extends  MyhiveBaseVisitor<String> {
    @Override
    public String visitSelect_stmt_1(MyhiveParser.Select_stmt_1Context ctx) {
        return super.visitSelect_stmt_1(ctx);
    }

    @Override
    public String visitSelect_stmt_2(MyhiveParser.Select_stmt_2Context ctx) {
        System.out.println("A拿到了NAME："+ctx.NAME());
        System.out.println("A拿到了col_name："+ctx.col_name());
        return super.visitSelect_stmt_2(ctx);
    }

    @Override
    public String visitCol_name(MyhiveParser.Col_nameContext ctx) {
        System.out.println("B拿到了NAME："+ctx.NAME());
        return super.visitCol_name(ctx);
    }
}
