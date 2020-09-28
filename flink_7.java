* @author camila.silveira
 */
public class meuMain {
   
    public static void main(String []args) throws Exception{
       
        //creat stremexecution environment nese caso local
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
       
        DataStream<String> arquivobase = see.readTextFile("/home/Disciplinas/Frameworks/flink/ocorrencias_criminais.csv");
       
        System.out.println("7 - para crimes do tipo narcotics , a cada 100 ocorridos no dia 1, agrupa-los de acordo com o mes exibindo a soma por mes");
       
        arquivobase.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> clctr) throws Exception {
                if (in.split(";")[4].equals("NARCOTICS") && Integer.valueOf(in.split(";")[0]) == 1){
                    clctr.collect(in);
                }

            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String t) throws Exception {
                return t.split(";")[1];
            }
        }).countWindowAll(100).aggregate(new AggregateFunction<String, HashMap<String, Long>, HashMap<String, Long>>() {
            @Override
            public HashMap<String, Long> createAccumulator() {
                return new HashMap<String, Long>();
            }

            @Override
            public HashMap<String, Long> add(String in, HashMap<String, Long> acc) {
                if (acc.containsKey(in)) {
                    acc.put(in, acc.get(in) + 1l);
                } else {
                    acc.put(in, 1l);
                }
                return acc;
            }

            @Override  
            public HashMap<String, Long> getResult(HashMap<String, Long> acc) {
                return acc;
            }

            @Override
            public HashMap<String, Long> merge(HashMap<String, Long> acc, HashMap<String, Long> acc1) {
                for (Map.Entry<String, Long> entrada : acc1.entrySet()) {
                    if (acc.containsKey(entrada.getKey())) {
                        acc.put(entrada.getKey(), acc.get(entrada.getKey()) +acc1.get(entrada.getKey()));
                    } else {
                        acc.put(entrada.getKey(), entrada.getValue());
                    }
                }
                return acc;
            }
        }).print();
       
        see.execute();
    }
   
}
   


7 - para crimes do tipo narcotics , a cada 100 ocoridos no dia 1, agrupa-los de acordo com o mes exibindo a soma por mes
4> {08=20, 10=80}
5> {12=25, 02=4, 05=7, 06=1, 10=63}
7> {12=8, 02=5, 03=1, 06=1, 08=6, 09=50, 10=29}
3> {01=1, 12=9, 02=4, 03=8, 05=11, 07=2, 08=4, 09=6, 10=55}
13> {11=10, 01=4, 02=32, 03=1, 05=7, 06=21, 07=24, 09=1}
12> {11=7, 05=22, 09=71}
8> {04=3, 08=11, 09=38, 10=48}
6> {01=10, 12=1, 02=3, 07=1, 09=6, 10=79}
19> {01=1, 03=13, 05=5, 06=79, 07=1, 10=1}