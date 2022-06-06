import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class TokenTest {
    public static void main(String[] args) {
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!" ;
        System.out.println(ToAnalysis.parse(str));
        Result result = ToAnalysis.parse(str);
        for(Term term :result.getTerms()){
            if(term.getName().length()>1) {
                System.out.println(term.getName());
            }
        }
    }
}
