// Generated from Myhive.g4 by ANTLR 4.9.3
package antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MyhiveParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.9.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, INT=4, WS=5, NAME=6, PLUS=7;
	public static final int
		RULE_select_stmt_1 = 0, RULE_select_stmt_2 = 1, RULE_col_name = 2;
	private static String[] makeRuleNames() {
		return new String[] {
			"select_stmt_1", "select_stmt_2", "col_name"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'select * from students'", "'select'", "'from'", null, null, null, 
			"'+'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, "INT", "WS", "NAME", "PLUS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Myhive.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public MyhiveParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class Select_stmt_1Context extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(MyhiveParser.EOF, 0); }
		public Select_stmt_1Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_stmt_1; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).enterSelect_stmt_1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).exitSelect_stmt_1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MyhiveVisitor ) return ((MyhiveVisitor<? extends T>)visitor).visitSelect_stmt_1(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_stmt_1Context select_stmt_1() throws RecognitionException {
		Select_stmt_1Context _localctx = new Select_stmt_1Context(_ctx, getState());
		enterRule(_localctx, 0, RULE_select_stmt_1);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(6);
			match(T__0);
			setState(7);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_stmt_2Context extends ParserRuleContext {
		public Col_nameContext col_name() {
			return getRuleContext(Col_nameContext.class,0);
		}
		public TerminalNode NAME() { return getToken(MyhiveParser.NAME, 0); }
		public TerminalNode EOF() { return getToken(MyhiveParser.EOF, 0); }
		public Select_stmt_2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_stmt_2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).enterSelect_stmt_2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).exitSelect_stmt_2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MyhiveVisitor ) return ((MyhiveVisitor<? extends T>)visitor).visitSelect_stmt_2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Select_stmt_2Context select_stmt_2() throws RecognitionException {
		Select_stmt_2Context _localctx = new Select_stmt_2Context(_ctx, getState());
		enterRule(_localctx, 2, RULE_select_stmt_2);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(9);
			match(T__1);
			setState(10);
			col_name();
			setState(11);
			match(T__2);
			setState(12);
			match(NAME);
			setState(13);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Col_nameContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(MyhiveParser.NAME, 0); }
		public Col_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_col_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).enterCol_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MyhiveListener ) ((MyhiveListener)listener).exitCol_name(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MyhiveVisitor ) return ((MyhiveVisitor<? extends T>)visitor).visitCol_name(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Col_nameContext col_name() throws RecognitionException {
		Col_nameContext _localctx = new Col_nameContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_col_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(15);
			match(NAME);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\t\24\4\2\t\2\4\3"+
		"\t\3\4\4\t\4\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\4\2\2\5\2\4"+
		"\6\2\2\2\20\2\b\3\2\2\2\4\13\3\2\2\2\6\21\3\2\2\2\b\t\7\3\2\2\t\n\7\2"+
		"\2\3\n\3\3\2\2\2\13\f\7\4\2\2\f\r\5\6\4\2\r\16\7\5\2\2\16\17\7\b\2\2\17"+
		"\20\7\2\2\3\20\5\3\2\2\2\21\22\7\b\2\2\22\7\3\2\2\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}