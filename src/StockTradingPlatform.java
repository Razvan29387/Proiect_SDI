import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

// ==================== EVENT SYSTEM ====================

abstract class TradingEvent implements Serializable {
    private UUID originNodeId;
    public void setOriginNodeId(UUID originNodeId) { this.originNodeId = originNodeId; }
    public UUID getOriginNodeId() { return originNodeId; }
}

class PriceUpdateEvent extends TradingEvent {
    private final String symbol;
    private final double price;
    public PriceUpdateEvent(String symbol, double price) {
        this.symbol = symbol;
        this.price = price;
    }
    public String getSymbol() { return symbol; }
    public double getPrice() { return price; }
}

// Event to request the server to start tracking a new symbol
class SymbolRequestEvent extends TradingEvent {
    private final String symbol;
    public SymbolRequestEvent(String symbol) { this.symbol = symbol; }
    public String getSymbol() { return symbol; }
}

// P2P Limit Order (Buy/Sell from other users)
class OrderEvent extends TradingEvent {
    enum OrderType { BUY, SELL }
    enum OrderStatus { PENDING, EXECUTED, CANCELLED }

    private final String userId;
    private final String symbol;
    private final OrderType type;
    private final double price;
    private final int quantity;
    private OrderStatus status;

    public OrderEvent(String userId, String symbol, OrderType type, double price, int quantity) {
        this.userId = userId;
        this.symbol = symbol;
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        this.status = OrderStatus.PENDING;
    }
    public String getUserId() { return userId; }
    public String getSymbol() { return symbol; }
    public OrderType getType() { return type; }
    public double getPrice() { return price; }
    public int getQuantity() { return quantity; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
}

// Direct Order (Buy/Sell from the "Bank" / Market Maker)
class DirectOrderEvent extends TradingEvent {
    enum Type { BUY, SELL }
    private final String userId;
    private final String symbol;
    private final Type type;
    private final int quantity;

    public DirectOrderEvent(String userId, String symbol, Type type, int quantity) {
        this.userId = userId;
        this.symbol = symbol;
        this.type = type;
        this.quantity = quantity;
    }
    public String getUserId() { return userId; }
    public String getSymbol() { return symbol; }
    public Type getType() { return type; }
    public int getQuantity() { return quantity; }
}

class TradeExecutedEvent extends TradingEvent {
    private final String symbol;
    private final double price;
    private final int quantity;
    private final String buyerId;
    private final String sellerId;

    public TradeExecutedEvent(String symbol, double price, int quantity, String buyerId, String sellerId) {
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
    }
    public String getSymbol() { return symbol; }
    public double getPrice() { return price; }
    public int getQuantity() { return quantity; }
    public String getBuyerId() { return buyerId; }
    public String getSellerId() { return sellerId; }
}

// ==================== INFRASTRUCTURE ====================

class DistributedEventBus {
    private final Map<Class<? extends TradingEvent>, List<EventListener<? extends TradingEvent>>> listeners;
    private final ExecutorService executorService;
    private final NetworkService networkService;
    private final UUID nodeId = UUID.randomUUID();

    public DistributedEventBus() {
        this.listeners = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
        try {
            this.networkService = new NetworkService(this, "230.0.0.0", 8888);
            new Thread(networkService::listen, "NetworkListener").start();
        } catch (IOException e) {
            throw new RuntimeException("Network init failed", e);
        }
    }

    public UUID getNodeId() { return nodeId; }

    public <T extends TradingEvent> void subscribe(Class<T> eventType, EventListener<T> listener) {
        listeners.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    void publishLocally(TradingEvent event) {
        List<EventListener<? extends TradingEvent>> eventListeners = listeners.get(event.getClass());
        if (eventListeners != null) {
            for (EventListener<? extends TradingEvent> listener : eventListeners) {
                executorService.submit(() -> {
                    try {
                        ((EventListener<TradingEvent>) listener).onEvent(event);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public void publish(TradingEvent event) {
        event.setOriginNodeId(nodeId);
        publishLocally(event);
        networkService.send(event);
    }

    public void shutdown() {
        executorService.shutdownNow();
        networkService.close();
    }
}

interface EventListener<T extends TradingEvent> {
    void onEvent(T event);
}

class NetworkService {
    private final DistributedEventBus localEventBus;
    private final MulticastSocket socket;
    private final InetAddress group;
    private final int port;
    private volatile boolean running = true;

    public NetworkService(DistributedEventBus eventBus, String multicastAddress, int port) throws IOException {
        this.localEventBus = eventBus;
        this.port = port;
        this.socket = new MulticastSocket(port);
        this.group = InetAddress.getByName(multicastAddress);
        
        NetworkInterface networkInterface = findBestInterface();
        System.out.println("Network: Binding to interface " + networkInterface.getDisplayName());

        this.socket.joinGroup(new InetSocketAddress(group, port), networkInterface);
        this.socket.setLoopbackMode(false);
    }

    private NetworkInterface findBestInterface() throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isLoopback() || !ni.isUp()) continue;
            Enumeration<InetAddress> addresses = ni.getInetAddresses();
            while(addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address) {
                    return ni;
                }
            }
        }
        try {
            return NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
        } catch (UnknownHostException e) {
            return NetworkInterface.getNetworkInterfaces().nextElement();
        }
    }

    public void send(TradingEvent event) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(event);
            byte[] data = baos.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length, group, port);
            socket.send(packet);
        } catch (IOException e) {
            System.err.println("Send error: " + e.getMessage());
        }
    }

    public void listen() {
        byte[] buffer = new byte[65535];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(packet.getData(), packet.getOffset(), packet.getLength()))) {
                    TradingEvent event = (TradingEvent) ois.readObject();
                    if (event.getOriginNodeId() != null && event.getOriginNodeId().equals(localEventBus.getNodeId())) continue;
                    localEventBus.publishLocally(event);
                }
            } catch (Exception e) {
                if (running) System.err.println("Receive error: " + e.getMessage());
            }
        }
    }

    public void close() {
        running = false;
        socket.close();
    }
}

// ==================== DATA FETCHING ====================

interface PriceFetcher {
    double fetchPrice(String symbol) throws Exception;
}

class StooqStockFetcher implements PriceFetcher {
    public double fetchPrice(String symbol) throws Exception {
        String urlStr = "https://stooq.com/q/l/?s=" + symbol + "&f=sd2t2ohlc&h&e=csv";
        URL url = new URL(urlStr);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            String header = reader.readLine();
            String data = reader.readLine();
            if (data != null) {
                String[] parts = data.split(",");
                if (parts.length >= 7) {
                    String closePrice = parts[6];
                    if (!closePrice.equals("N/D")) {
                        return Double.parseDouble(closePrice);
                    }
                }
            }
        }
        throw new IOException("Could not fetch stock data for " + symbol);
    }
}

class BinanceCryptoFetcher implements PriceFetcher {
    public double fetchPrice(String symbol) throws Exception {
        String urlStr = "https://api.binance.com/api/v3/ticker/price?symbol=" + symbol;
        URL url = new URL(urlStr);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            StringBuilder json = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) json.append(line);
            
            String jsonStr = json.toString();
            int priceIndex = jsonStr.indexOf("\"price\":\"");
            if (priceIndex != -1) {
                int start = priceIndex + 9;
                int end = jsonStr.indexOf("\"", start);
                String priceStr = jsonStr.substring(start, end);
                return Double.parseDouble(priceStr);
            }
        }
        throw new IOException("Could not fetch crypto data for " + symbol);
    }
}

class RealTimePriceGenerator implements Runnable {
    private final String symbol;
    private final PriceFetcher fetcher;
    private final DistributedEventBus bus;
    private final long intervalMs;

    public RealTimePriceGenerator(String symbol, PriceFetcher fetcher, DistributedEventBus bus, long intervalMs) {
        this.symbol = symbol;
        this.fetcher = fetcher;
        this.bus = bus;
        this.intervalMs = intervalMs;
    }

    public void run() {
        while (true) {
            try {
                double price = fetcher.fetchPrice(symbol);
                bus.publish(new PriceUpdateEvent(symbol, price));
                System.out.println("SERVER: Updated " + symbol + " -> " + price);
            } catch (Exception e) {
                System.err.println("SERVER: Error fetching " + symbol + ": " + e.getMessage());
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}

// ==================== SERVER COMPONENTS ====================

class MarketServer {
    private final DistributedEventBus eventBus;
    private final Map<String, Double> currentPrices = new ConcurrentHashMap<>();
    private final OrderMatchingEngine matchingEngine;
    private final Set<String> activeSymbols = ConcurrentHashMap.newKeySet();

    public MarketServer(DistributedEventBus eventBus) {
        this.eventBus = eventBus;
        this.matchingEngine = new OrderMatchingEngine(eventBus);
        
        eventBus.subscribe(OrderEvent.class, matchingEngine);
        eventBus.subscribe(DirectOrderEvent.class, this::handleDirectOrder);
        eventBus.subscribe(PriceUpdateEvent.class, e -> currentPrices.put(e.getSymbol(), e.getPrice()));
        eventBus.subscribe(SymbolRequestEvent.class, this::handleSymbolRequest);
        
        startDefaultFeeds();
    }

    private void startDefaultFeeds() {
        // Start with a few popular ones
        addSymbolTracker("AAPL.US");
        addSymbolTracker("BTCUSDT");
    }

    private void handleSymbolRequest(SymbolRequestEvent event) {
        String symbol = event.getSymbol().toUpperCase();
        if (activeSymbols.contains(symbol)) {
            System.out.println("SERVER: Already tracking " + symbol);
            return;
        }
        System.out.println("SERVER: Received request to track " + symbol);
        addSymbolTracker(symbol);
    }

    private void addSymbolTracker(String symbol) {
        if (activeSymbols.contains(symbol)) return;
        activeSymbols.add(symbol);

        // Determine type based on format
        // Binance usually ends in USDT, BTC, ETH, etc. Stooq usually has a dot (AAPL.US)
        if (symbol.contains(".")) {
            // Assume Stock (Stooq)
            new Thread(new RealTimePriceGenerator(symbol, new StooqStockFetcher(), eventBus, 15000)).start();
            System.out.println("SERVER: Started Stock Tracker for " + symbol);
        } else {
            // Assume Crypto (Binance)
            new Thread(new RealTimePriceGenerator(symbol, new BinanceCryptoFetcher(), eventBus, 5000)).start();
            System.out.println("SERVER: Started Crypto Tracker for " + symbol);
        }
    }

    private void handleDirectOrder(DirectOrderEvent order) {
        Double price = currentPrices.get(order.getSymbol());
        if (price == null) {
            System.out.println("SERVER: Price not found for " + order.getSymbol());
            return; 
        }

        String buyer = order.getType() == DirectOrderEvent.Type.BUY ? order.getUserId() : "BANK";
        String seller = order.getType() == DirectOrderEvent.Type.SELL ? order.getUserId() : "BANK";

        TradeExecutedEvent trade = new TradeExecutedEvent(
                order.getSymbol(), price, order.getQuantity(), buyer, seller
        );
        System.out.println("SERVER: Executing Direct Trade for " + order.getUserId());
        eventBus.publish(trade);
    }
}

class OrderMatchingEngine implements EventListener<OrderEvent> {
    private final DistributedEventBus bus;
    private final Map<String, PriorityQueue<OrderEvent>> buyOrders = new ConcurrentHashMap<>();
    private final Map<String, PriorityQueue<OrderEvent>> sellOrders = new ConcurrentHashMap<>();

    public OrderMatchingEngine(DistributedEventBus bus) { this.bus = bus; }

    public void onEvent(OrderEvent order) {
        System.out.println("SERVER: Processing P2P Order: " + order.getType() + " " + order.getSymbol());
        synchronized (this) {
            if (order.getType() == OrderEvent.OrderType.BUY) match(order, buyOrders, sellOrders, true);
            else match(order, sellOrders, buyOrders, false);
        }
    }

    private void match(OrderEvent order, Map<String, PriorityQueue<OrderEvent>> myBook, 
                       Map<String, PriorityQueue<OrderEvent>> otherBook, boolean isBuy) {
        String sym = order.getSymbol();
        
        myBook.computeIfAbsent(sym, k -> new PriorityQueue<>(
            isBuy ? (a,b) -> Double.compare(b.getPrice(), a.getPrice()) : Comparator.comparingDouble(OrderEvent::getPrice)
        )).add(order);
        
        PriorityQueue<OrderEvent> buys = buyOrders.get(sym);
        PriorityQueue<OrderEvent> sells = sellOrders.get(sym);
        
        if (buys == null || sells == null) return;
        
        while (!buys.isEmpty() && !sells.isEmpty() && buys.peek().getPrice() >= sells.peek().getPrice()) {
            OrderEvent buy = buys.poll();
            OrderEvent sell = sells.poll();
            
            if (buy.getUserId().equals(sell.getUserId())) break;

            double execPrice = (buy.getPrice() + sell.getPrice()) / 2;
            int qty = Math.min(buy.getQuantity(), sell.getQuantity());
            
            bus.publish(new TradeExecutedEvent(sym, execPrice, qty, buy.getUserId(), sell.getUserId()));
        }
    }
}

// ==================== CLIENT ====================

class TraderClient implements EventListener<TradingEvent> {
    private final String userId;
    private final DistributedEventBus bus;
    private final Map<String, Integer> portfolio = new ConcurrentHashMap<>();
    private double balance = 10000.0;
    private final Map<String, Double> marketPrices = new ConcurrentHashMap<>();
    
    private final AtomicBoolean watching = new AtomicBoolean(false);

    public TraderClient(String userId, DistributedEventBus bus) {
        this.userId = userId;
        this.bus = bus;
        bus.subscribe(PriceUpdateEvent.class, this::onPrice);
        bus.subscribe(TradeExecutedEvent.class, this::onTrade);
    }

    private void onPrice(PriceUpdateEvent e) {
        marketPrices.put(e.getSymbol(), e.getPrice());
        if (watching.get()) {
            System.out.printf("[LIVE] %s: %.2f%n", e.getSymbol(), e.getPrice());
        }
    }

    private void onTrade(TradeExecutedEvent e) {
        String buyer = e.getBuyerId().equals(userId) ? "YOU" : e.getBuyerId();
        String seller = e.getSellerId().equals(userId) ? "YOU" : e.getSellerId();
        double totalValue = e.getPrice() * e.getQuantity();

        if (e.getBuyerId().equals(userId)) {
            balance -= totalValue;
            portfolio.merge(e.getSymbol(), e.getQuantity(), Integer::sum);
            System.out.printf("%n[+] YOU bought from %s %s %d with %.2f%n> ", 
                seller, e.getSymbol(), e.getQuantity(), totalValue);
        } else if (e.getSellerId().equals(userId)) {
            balance += totalValue;
            portfolio.merge(e.getSymbol(), -e.getQuantity(), Integer::sum);
            System.out.printf("%n[-] YOU sold to %s %s %d with %.2f%n> ", 
                buyer, e.getSymbol(), e.getQuantity(), totalValue);
        }
    }

    public void startInteractive() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Welcome " + userId + "!");
        System.out.println("Type 'help' for commands.");
        
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line.trim().isEmpty()) continue;
            
            String[] parts = line.trim().split("\\s+");
            String cmd = parts[0].toLowerCase();

            try {
                switch (cmd) {
                    case "add":
                        if (parts.length < 2) {
                            System.out.println("Usage: add <symbol> (e.g., add NVDA.US or add DOGEUSDT)");
                            break;
                        }
                        String newSym = parts[1].toUpperCase();
                        bus.publish(new SymbolRequestEvent(newSym));
                        System.out.println("Requested server to track: " + newSym);
                        break;
                    case "buy": // P2P
                        if (parts.length > 1 && parts[1].equalsIgnoreCase("direct")) {
                            System.out.println("[!] Did you mean 'buy-direct'? Usage: buy-direct <sym> <qty>");
                            break;
                        }
                        if (parts.length < 4) {
                            System.out.println("Usage: buy <sym> <price> <qty>");
                            break;
                        }
                        bus.publish(new OrderEvent(userId, parts[1].toUpperCase(), OrderEvent.OrderType.BUY, Double.parseDouble(parts[2]), Integer.parseInt(parts[3])));
                        System.out.println("Placed Limit Buy Order.");
                        break;
                    case "sell": // P2P
                        if (parts.length > 1 && parts[1].equalsIgnoreCase("direct")) {
                            System.out.println("[!] Did you mean 'sell-direct'? Usage: sell-direct <sym> <qty>");
                            break;
                        }
                        if (parts.length < 4) {
                            System.out.println("Usage: sell <sym> <price> <qty>");
                            break;
                        }
                        String sellSym = parts[1].toUpperCase();
                        int sellQty = Integer.parseInt(parts[3]);
                        if (portfolio.getOrDefault(sellSym, 0) < sellQty) {
                            System.out.println("⚠️ EROARE: Nu ai suficiente actiuni " + sellSym + " pentru a vinde! (Detii: " + portfolio.getOrDefault(sellSym, 0) + ")");
                            break;
                        }
                        bus.publish(new OrderEvent(userId, sellSym, OrderEvent.OrderType.SELL, Double.parseDouble(parts[2]), sellQty));
                        System.out.println("Placed Limit Sell Order.");
                        break;
                    case "buy-direct": // Bank
                        if (parts.length < 3) {
                            System.out.println("Usage: buy-direct <sym> <qty>");
                            break;
                        }
                        bus.publish(new DirectOrderEvent(userId, parts[1].toUpperCase(), DirectOrderEvent.Type.BUY, Integer.parseInt(parts[2])));
                        System.out.println("Sent Direct Buy Request.");
                        break;
                    case "sell-direct": // Bank
                        if (parts.length < 3) {
                            System.out.println("Usage: sell-direct <sym> <qty>");
                            break;
                        }
                        String dirSellSym = parts[1].toUpperCase();
                        int dirSellQty = Integer.parseInt(parts[2]);
                        if (portfolio.getOrDefault(dirSellSym, 0) < dirSellQty) {
                            System.out.println("⚠️ EROARE: Nu ai suficiente actiuni " + dirSellSym + " pentru a vinde! (Detii: " + portfolio.getOrDefault(dirSellSym, 0) + ")");
                            break;
                        }
                        bus.publish(new DirectOrderEvent(userId, dirSellSym, DirectOrderEvent.Type.SELL, dirSellQty));
                        System.out.println("Sent Direct Sell Request.");
                        break;
                    case "port":
                        System.out.println("Portfolio: " + portfolio + " | Cash: " + balance);
                        break;
                    case "ticker":
                        System.out.println("--- MARKET SNAPSHOT ---");
                        System.out.println("STOCKS:");
                        marketPrices.entrySet().stream()
                            .filter(e -> e.getKey().contains("."))
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(e -> System.out.printf("  %-10s : %.2f%n", e.getKey(), e.getValue()));
                        System.out.println("CRYPTO:");
                        marketPrices.entrySet().stream()
                            .filter(e -> !e.getKey().contains("."))
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(e -> System.out.printf("  %-10s : %.2f%n", e.getKey(), e.getValue()));
                        System.out.println("-----------------------");
                        break;
                    case "watch":
                        System.out.println("--- LIVE MARKET (Press ENTER to stop) ---");
                        watching.set(true);
                        scanner.nextLine();
                        watching.set(false);
                        System.out.println("--- STOPPED WATCHING ---");
                        break;
                    case "help":
                        System.out.println("Commands:");
                        System.out.println("  add <sym>                     (Request server to track a new symbol, e.g., NVDA.US or DOGEUSDT)");
                        System.out.println("  ticker                        (Show current prices for Stocks & Crypto)");
                        System.out.println("  watch                         (Stream live prices - Press Enter to stop)");
                        System.out.println("  buy <sym> <price> <qty>       (Trade with other players)");
                        System.out.println("  sell <sym> <price> <qty>      (Trade with other players)");
                        System.out.println("  buy-direct <sym> <qty>        (Buy from Bank at market price)");
                        System.out.println("  sell-direct <sym> <qty>       (Sell to Bank at market price)");
                        System.out.println("  port                          (Show portfolio)");
                        break;
                    case "exit":
                        System.exit(0);
                    default:
                        System.out.println("Unknown command.");
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }

    @Override
    public void onEvent(TradingEvent event) {
        // Handled by specific methods
    }
}

// ==================== MAIN ====================

public class StockTradingPlatform {
    public static void main(String[] args) throws IOException {
        DistributedEventBus bus = new DistributedEventBus();

        // 1. AUTOMATED MODE (via Script)
        if (args.length > 0) {
            if (args[0].equalsIgnoreCase("server")) {
                System.out.println("=== SERVER MODE STARTED ===");
                new MarketServer(bus);
                System.out.println("Server running... (Press Ctrl+C to stop)");
                new Scanner(System.in).nextLine();
                return;
            } else if (args[0].equalsIgnoreCase("client") && args.length > 1) {
                String username = args[1];
                System.out.println("=== CLIENT MODE STARTED: " + username + " ===");
                new TraderClient(username, bus).startInteractive();
                return;
            }
        }

        // 2. MANUAL MODE (Interactive)
        System.out.println("=== DISTRIBUTED STOCK PLATFORM ===");
        System.out.println("Select Mode:");
        System.out.println("1. Server (Engine + Market Maker)");
        System.out.println("2. Client (Trader)");
        System.out.print("Choice [1/2]: ");

        Scanner scanner = new Scanner(System.in);
        String choice = scanner.nextLine();

        if (choice.equals("1")) {
            System.out.println("Starting Server...");
            new MarketServer(bus);
            System.out.println("Server running. Press Enter to stop.");
            scanner.nextLine();
        } else {
            System.out.print("Enter Username: ");
            String user = scanner.nextLine();
            new TraderClient(user, bus).startInteractive();
        }
        
        bus.shutdown();
    }
}
