import groovy.sql.Sql

dbHandle = null
outputColumn = []

def getDb() {
  if (dbHandle) return dbHandle
  dbHandle = Sql.newInstance("jdbc:mysql://$GlobalConf.host:$GlobalConf.port/oceanbase", GlobalConf.username, GlobalConf.password, "com.mysql.jdbc.Driver")
  return dbHandle
}

def listTenant() {
  println "TENANT NAME         ID"
  println "-----------         ----"
  getDb().rows("""select tenant_name, tenant_id from __all_tenant;""").each { r ->
    println "${r.tenant_name.padRight(20)}${r.tenant_id}"
  }
  System.exit(0)
}

def listStat() {
  println "STAT ID  DESC"
  println "-------  ------"
  getDb().rows("""select distinct stat_id, name from v\$sysstat;""").each { r ->
    println "${r.stat_id.toString().padLeft(7)}  ${r.name}"
  }
  System.exit(0)
}

class OutLine {
  String l = ""
  public def add(String s, int width) {
    l += s.padLeft(width)
    l += " "
  }
  public def add(String s, int scale, int width) {
    add(s, width)
  }
  public def add(BigDecimal v, int scale, int width) {
    while (scale > 0) if (v >= 10 ** (width - scale - 1)) scale--; else break;
    add(v.setScale(scale, BigDecimal.ROUND_HALF_UP).toString(), width)
  }
  public def add(long v, int width) {
    add(v.toString(), width)
  }
  public def out() {
    println l
    l = ""
  }
}

class OutputColumn {
  String name
  int    width
  int    scale
  def    script
  OutputColumn(name, width, scale, formular) {
    this.name = name
    this.width = width
    this.scale = scale
    this.script = new GroovyShell().parse(formular)
  }
  def getRes(b) {
    try {
      script.binding = b
      return script.run()
    } catch (java.lang.ArithmeticException e) {
      return 0
    } catch (groovy.lang.MissingPropertyException e) {
      return 0
    }
  }
}

class Output {
  List columns = []
  int lineCount = 0
  void addColumn(c) {
    if (c.name) {
      columns.add(new OutputColumn(c.name, c.width, c.scale ? c.scale : 0, c.formular))
    }
  }
  String out(b) {
    OutLine o = new OutLine()
    if (lineCount % 20 == 0) {
      columns.each{
        o.add(it.name, it.width)
      }
      o.out()
      columns.each{
        o.add('-' * it.width, it.width)
      }
      o.out()
      lineCount = 0
    }
    columns.each{
      o.add(it.getRes(b), it.scale, it.width)
    }
    o.out()
    lineCount ++
  }
}


def addBinding(b, name, curValue, preValue) {
  b."${name}" = curValue
  b."PREVIOUS_${name}" = preValue
  b."${name}_DIFF" = curValue - preValue
}

def get() {
  Map preStat = [:]
  Map curStat = [:]
  Date curTime
  long curTimestamp

  Output out = new Output()
  out.addColumn([name: "Time", formular: "CURRENT_TIME", width: GlobalConf.delay >= 1000 ? 14 : 16])
  outputColumn.each {
    out.addColumn(it)
  }

  Binding binding = new Binding()

  while (true) {
    curTime = new Date()
    curTimestamp = System.currentTimeMillis()
    try {
//where con_id = 1002 
      getDb().rows("""select stat_id, sum(value) as value from v\$sysstat group by stat_id;""").each { r ->
        curStat[r.stat_id] = r.value
      }
      if (preStat) {
        curStat.each {
          addBinding(binding, "S${it.key}", it.value, preStat[it.key])
          def name = GlobalConf.STAT_ID_NAME_MAP[it.key.toInteger()]
          if (name) addBinding(binding, name, it.value, preStat[it.key])
        }
        binding."CURRENT_TIME" = GlobalConf.delay >= 1000 ? curTime.format('yyyyMMddHHmmss') : curTime.format('yyyyMMddHHmmss.SSS')[0..15]
        binding."INTERVAL_SECONDS" = GlobalConf.delay.div(1000)

        out.out(binding)
      }
    } catch (Exception e) {
      println ("Connection Error: $GlobalConf.host:$GlobalConf.port\n\n$e")
      dbHandle = null
    }
    def d = GlobalConf.delay - (System.currentTimeMillis() - curTimestamp)
    if (d > 0) sleep(d.toLong())
    (preStat, curStat) = [curStat, preStat]
  }
}

def parseArgs(args) {
  def predefinedCategory = GlobalConf.COLUMN_DEF.inject([]) {l, it -> l + it.key}.join(' ')
  def cli = new CliBuilder(usage: "obstat [$predefinedCategory]")
  // Create the list of options.
  cli.with {
    h args: 1, argName: 'host', 'host address, default 127.0.0.1'
    u args: 1, argName: 'username', 'username, default root'
    p args: 1, argName: 'password',  'password, default ""'
    d args: 1, argName: 'delay', 'delay between updates in seconds, default 1'
    f args: 1, argName: 'column def file', 'new column defination'
    t 'list tenant'
    s 'list stat'
  }
  cli.P(args: 1, argName: 'port', 'OceanBase port, default 2828')
  cli._(longOpt: 'help', 'Show usage information')

  def options = cli.parse(args)
  if (!options) {
    System.exit(1)
  }
  if (options.help) {
    cli.usage()
    System.exit(0)
  }

  GlobalConf.host = options.h ? options.h : '127.0.0.1'
  GlobalConf.port = options.P ? options.P : '2828'
  GlobalConf.username = options.u ? options.u : 'root'
  GlobalConf.password = options.p ? options.p : ''
  GlobalConf.delay = (options.d ? options.d : 1).toBigDecimal() * 1000

  if (options.t) listTenant()
  if (options.s) listStat()

  def catList = options.arguments() ? options.arguments().unique() : ["general"]
  catList.each {
    if (GlobalConf.COLUMN_DEF[it]) outputColumn += GlobalConf.COLUMN_DEF[it]
    else println "Unknown Column Category: $it"
  }

  if (options.f) {
    try {
      def b = new Binding()
      new GroovyShell(b).evaluate(new File(options.f))
      outputColumn += b.COLUMN_DEF
    } catch (Exception e) {
      println "Wrong Column Defination File:  $e"
      System.exit(1)
    }
  }
}

parseArgs(args)

get()


