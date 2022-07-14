import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

//https://dumps.wikimedia.org/backup-index.html

//in build path: commons-compress-1.21.jar

public class WordIcebergs
{
	private static final int[] categoryCounts={10,50,100,500,1000,5000,10000};
	private Map<String,Set<String>> dictionaries=new HashMap<>();
	
	public static void main(String[] args) throws Exception
	{
		//https://github.com/kkrypt0nn/Wordlists/tree/master/languages
		//ar,cs,da,de,fr,hr,it,pt,ru
		
		//https://github.com/titoBouzout/Dictionaries/
		//be,br,ca,el,et,eu,fa,gl,he,hi,hr,hu,hy,id,is,ka,la,lb,lt,lv,mn,ms,nl,no,oc,pl,ro,sk,sl,sr,sv,tr,uk,vi
		
		//https://github.com/MinhasKamal/BengaliDictionary/blob/master/BengaliWordList_439.txt
		//https://github.com/dwyl/english-words/
		//https://github.com/JorgeDuenasLerin/diccionario-espanol-txt
		//https://github.com/Shreeshrii/hindi-hunspell/blob/master/Hindi/hi_IN.dic
		//https://www.edrdg.org/wiki/index.php/JMdict-EDICT_Dictionary_Project
		//https://github.com/acidsound/korean_wordlist/blob/master/wordslist.txt
		//https://github.com/urduhack/urdu-words/blob/master/words.txt
		
		WordIcebergs wi=new WordIcebergs();
		wi.runAll("input/");
		wi.analyzeAll();
	}

	private void analyzeAll() throws IOException
	{
		Map<String,List<String>> languageStats=new HashMap<>();
		Pattern statPattern=Pattern.compile("(able to read|word count:) (\\d+)");
		
		for(File file : new File("output/").listFiles())
		{
			String filename=file.getName();
			
			if(filename.endsWith("wikibooks.txt"))
			{
				String language=filename.substring(0,2);
				
				List<String> stats=Files.lines(file.toPath())
					.filter(line -> line.contains("(able to read") || line.contains("word count: "))
					.map(line -> statPattern.matcher(line))
					.filter(matcher -> matcher.find())
					.map(matcher -> matcher.group(2))
					.collect(Collectors.toList());
				
				languageStats.put(language,stats);
			}
		}
		
		System.out.print("lang\tcorpus words\tunique corpus words");
		Arrays.stream(categoryCounts)
			.forEach(cc -> System.out.print("\t"+cc));
		System.out.println();
		
		languageStats.entrySet().stream()
			.map(e -> e.getKey()+"\t"+e.getValue().toString().replace(", ","\t").replaceAll("\\[|\\]",""))
			.sorted()
			.forEach(System.out::println);
	}

	private void runAll(String dir) throws Exception
	{
		for(String filename : new File(dir).list())
		{
			//if wikibooks index
			if(filename.contains("wikibooks") && filename.endsWith("multistream-index.txt.bz2"))
			{
				String language=filename.substring(0,2);
				
				if(new File("output/"+language+"wikibooks"+".txt").exists())
				{
					System.out.println("skip "+language+", already exists");
					continue;
				}
				
				Set<String> dictionary=getDictionary(language,dir);
				
				File index=new File(dir,filename);
				File multistream=new File(dir,filename.replace("-index.txt.bz2",".xml.bz2"));
				long start=System.currentTimeMillis();
				runMultistream(language,dictionary,index,multistream);
				long ms=System.currentTimeMillis()-start;
				System.out.println(ms+"ms");
			}
		}
	}

	private Set<String> getDictionary(String language, String dir) throws IOException
	{
		if(dictionaries.containsKey(language))
		{
			return dictionaries.get(language);
		}
		
		Set<String> dictionary=getDictionaryRaw(language,dir);
		dictionaries.put(language,dictionary);
		return dictionary;
	}
	
	private Set<String> getDictionaryRaw(String language, String dir) throws IOException
	{
		System.out.println(language);
		File parent=new File(dir);
		String filename=language+"dictionary";
		
		File json=new File(parent,filename+".json");
		if(json.exists())
		{
			String[] words=Files.readAllLines(json.toPath()).get(0).split("[\\[\\]\\\",]+");
			return Arrays.asList(words).stream()
					.collect(Collectors.toSet());
		}
		
		File xml=new File(parent,filename+".xml");
		if(xml.exists())
		{
			Pattern pattern=Pattern.compile("<.eb>.*</.eb>");
			
			return Files.lines(xml.toPath())
				.filter(l -> pattern.matcher(l).matches())
				.map(l -> l.replaceAll("</?.eb>",""))
				.collect(Collectors.toSet());
		}
		
		File txt=new File(parent,filename+".txt");
		return Files.lines(txt.toPath())
				.map(line -> line.contains("/")?line.substring(0,line.indexOf('/')):line)
				.collect(Collectors.toSet());
	}

	private void runMultistream(String language, Set<String> dictionary, File index, File multistream) throws Exception
	{
		System.out.println(index.getAbsolutePath());
		Set<Long> offsets=new HashSet<>();
		
		try(FileInputStream fis=new FileInputStream(index))
		{
			try(BufferedInputStream bis=new BufferedInputStream(fis);
				CompressorInputStream cis=new CompressorStreamFactory().createCompressorInputStream(bis);
					BufferedReader br=new BufferedReader(new InputStreamReader(cis))
				)
			{
				String line=null;
				
				while((line=br.readLine())!=null)
				{
					String offset=line.substring(0,line.indexOf(':'));
					offsets.add(Long.parseLong(offset));
				}
			}
		}
		
		System.out.println(offsets.size());
		List<Long> sortedOffsets=offsets.stream()
				.sorted()
				.collect(Collectors.toList());
		Map<String,Long> wordCounts=new HashMap<>();
		
		for(int i=0; i<sortedOffsets.size(); i++)
		{
			System.out.println(i+" / "+sortedOffsets.size()+" , words="+wordCounts.size());
			
			try(FileInputStream fis=new FileInputStream(multistream))
			{
				fis.skip(sortedOffsets.get(i));
				
				try(BufferedInputStream bis=new BufferedInputStream(fis);
					CompressorInputStream cis=new CompressorStreamFactory().createCompressorInputStream(bis);
						BufferedReader br=new BufferedReader(new InputStreamReader(cis))
					)
				{
					boolean isInText=false;
					String line=null;
					
					while((line=br.readLine())!=null)
					{
						if(line.length()>9999)
						{
							continue;
						}
						
						if(line.contains("<text"))
						{
							isInText=true;
							runString(language,dictionary,wordCounts,line.substring(line.indexOf('>')));
						}
						else if(line.contains("</text>"))
						{
							isInText=false;
							runString(language,dictionary,wordCounts,line.substring(0,line.indexOf('<')));
						}
						else if(isInText)
						{
							runString(language,dictionary,wordCounts,line);
						}
					}
				}
			}
		}
		
		String filename=index.getName();
		writeResults(wordCounts,filename.substring(0,filename.indexOf('-'))+".txt");
	}
	
	private Pattern ignoreLine=Pattern.compile("[_∈=≠≡→⇒∞^\\|\\+\\*#∡]|\\b(f|g|sqrt|frac|tfrac|sqrtfrac|omega|gamma|lim|log|ln|cos|sin|tan|arctan|mbox|min|max|if|else|for|while|begin|end|color|math\\w+)_?\\s*[\\{\\(]|^File:|x_|!!|.\\s+.\\s+.\\s+.");
	
	private void runString(String language, Set<String> dictionary, Map<String,Long> wordCounts, String line)
	{
		line=line.replaceAll("(&lt;.*?&gt;)|(<.*?>)|(\\{\\{.*\\}\\})|(\\[.*\\])"
				+"|&quot;|&lt;|&gt;|&amp;|\\bii+\\b|\\w+=\\w+|\\\\.*?\\b","");
		
		if(ignoreLine.matcher(line).find())
		{
			return;
		}
		
		List<String> words="ja".equals(language)?getWordsJaZh(dictionary,line,jaWordPatterns)
				:"zh".equals(language)?getWordsJaZh(dictionary,line,zhWordPatterns)
				:getWords(dictionary,line);
		
		for(String word : words)
		{
			if(wordCounts.containsKey(word))
			{
				wordCounts.put(word,1L+wordCounts.get(word));
			}
			else
			{
				wordCounts.put(word,1L);
			}
		}
	}

	private List<String> getWords(Set<String> dictionary, String line)
	{
		List<String> words=new ArrayList<>();
		
		//split around letters/marks
		for(String word : line.split("[^\\p{L}\\p{M}']+"))
		{
			word=word.trim().toLowerCase();
			
			if(!word.isEmpty() && dictionary.contains(word))
			{
				words.add(word);
			}
		}
		
		return words;
	}
	
	private Pattern[] jaWordPatterns={
			Pattern.compile("\\p{IsHan}*\\p{IsHiragana}*"),
			Pattern.compile("\\p{IsKatakana}+")
	};
	private Pattern[] zhWordPatterns={
			Pattern.compile("\\p{IsHan}+")
	};

	private List<String> getWordsJaZh(Set<String> dictionary, String line, Pattern[] wordPatterns)
	{
		List<String> words=new ArrayList<>();
		
		//split on punctuation or whitespace/separator or number or A-Za-z
		for(String phrase : line.split("[\\p{P}\\p{Z}\\p{N}A-Za-z]+"))
		{
			if(!phrase.isEmpty())
			{
				for(Pattern pattern : wordPatterns)
				{
					Matcher matcher=pattern.matcher(phrase);
					
					while(matcher.find())
					{
						String group=matcher.group();
						addWordsJaZh(words,dictionary,group);
					}
				}
			}
		}
		
		return words;
	}

	private void addWordsJaZh(List<String> words, Set<String> dictionary, String group)
	{
		if(group.isBlank())
		{
			return;
		}
		
//		System.out.println(group);
		
		int maxWordFound=0;
		int mi0=-1, mi1=-1;
		
		for(int i0=0; i0<group.length(); i0++)
		{
			for(int i1=i0+maxWordFound+1; i1<=group.length(); i1++)
			{
				String substring=group.substring(i0,i1);
				
				if(dictionary.contains(substring))
				{
					maxWordFound=substring.length();
					mi0=i0;
					mi1=i1;
				}
			}
		}
		
		if(maxWordFound>0)
		{
			String substring=group.substring(mi0,mi1);
			words.add(substring);
//			System.out.println("> "+substring);
			
			addWordsJaZh(words,dictionary,group.substring(0,mi0));
			addWordsJaZh(words,dictionary,group.substring(mi1,group.length()));
		}
	}
	
	private void writeResults(Map<String,Long> wordCounts, String path) throws IOException
	{
		List<String> sortedWords=wordCounts.entrySet().stream()
			.sorted((e,f) -> f.getValue().compareTo(e.getValue()))
			.map(Entry::getKey)
			.collect(Collectors.toList());
		
		long corpusWordCount=wordCounts.values().stream()
				.mapToLong(i->i).sum();
		
		int cci=0;
		int i=0;
		int lineLength=0;
		long seenWordCount=0;
		
		try(BufferedWriter writer=Files.newBufferedWriter(Paths.get("output/"+path)))
		{
			writer.append("Corpus total word count: "+corpusWordCount);
			writer.append("\nCorpus unique word count: "+wordCounts.size()+"\n\n");
			StringBuilder section=new StringBuilder();
			
			for(String word : sortedWords)
			{
				if(lineLength+word.length()>=80)
				{
					section.append("\n");
					lineLength=0;
				}
				
				section.append(word+" ");
				seenWordCount+=wordCounts.get(word);
				lineLength+=word.length()+1;
				i++;
				
				if(i>=categoryCounts[cci])
				{
					String topNext=(cci==0)?"Top ":"\n\nNext ";
					int percent=(int)Math.round(seenWordCount*100./corpusWordCount);
					writer.append(topNext+categoryCounts[cci]+" words (able to read "+percent+"% of corpus)\n");
					writer.append(section);
					section.setLength(0);

					i=0;
					cci++;
					lineLength=0;
					
					if(cci>=categoryCounts.length)
					{
						break;
					}
				}
			}
			
			writer.append("\n");
		}
		
		System.out.println(sortedWords.size());
	}
}
