package knowledge.graph.visualization.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FruchtermanReingoldLayout {
    private int w; // 画布的宽度
    private int l; //画布的⻓长度
    private int temperature = w / 10; //模拟退火火初始温度
    private int maxIter = 1000; //算法迭代次数
    private int area; //布局大大小小
    private double c = 1; // 节点距离控制系数

    /**
     * init knowledge.graph.visualization.util.FruchtermanReingoldLayout
     * @param w the wide of graph
     * @param l the length of graph
     * @param maxIter the max iterator of the arig
     * @param rate define the initial value of temperature
     */
    public FruchtermanReingoldLayout(int w, int l, int maxIter, int rate, double c) {
        this.w = w;
        this.l = l;
        this.maxIter = maxIter;
        temperature = w/rate;
        this.c = c;
        this.area = w * l;
    }

    public List<Node> run(List<Node> nodes, List<Edge> edges) {
        List<Node> reSetNodes = nodes;
        for (int i = 0; i < maxIter; i++) {
            reSetNodes = springLayout(reSetNodes, edges, i);
        }
        return reSetNodes;
    }

    public List<Node> springLayout(List<Node> nodes, List<Edge> edges, int curIter) {
        //2计算每次迭代局部区域内两两节点间的斥力力力所产生生的单位位移(一一般为正值)
        double deltaX, deltaY, deltaLength;
        //节点之间的距离
        double k = c * Math.sqrt(area / (double) nodes.size());

        Map<Long, Double> dispX = new HashMap<>();
        Map<Long, Double> dispY = new HashMap<>();

        for (int v = 0; v < nodes.size(); v++) {
            dispX.put(nodes.get(v).getId(), 0.0);
            dispY.put(nodes.get(v).getId(), 0.0);
            for (int u = 0; u < nodes.size(); u++) {
                if (u != v) {
                    Node nodeV = nodes.get(v);
                    Node nodeU = nodes.get(u);
                    deltaX = nodeV.getX() - nodeU.getX();

                    if (Double.isNaN(deltaX)) {
                        System.out.println("x error" + nodes.get(v).getX());
                    }
                    deltaY = nodeV.getY() - nodeU.getY();

                    if (Double.isNaN(deltaY)) {
                        System.out.println("y error" + nodes.get(v).getX());
                    }
                    deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY) - (nodeV.getSize() + nodeU.getSize());

                    double force = k * k / deltaLength;

                    if (Double.isNaN(force)) {
                        System.err.println("force is NaN node is" + u + "->" + v + "diff length" + deltaLength + "x" + deltaX + "y" + deltaY);
                    }
                    Long id = nodes.get(v).getId();
                    dispX.put(id, dispX.get(id) + (deltaX / deltaLength) * force);
                    dispY.put(id, dispY.get(id) + (deltaY / deltaLength) * force);
                }
            }
        }

        //3. 计算每次迭代每条边的引力力力对两端节点所产生生的单位位移(一一般为负值)
        Node visnodeS = null, visnodeE = null;
        for (int e = 0; e < edges.size(); e++) {
            Long eStartID = edges.get(e).getSourceId();
            Long eEndID = edges.get(e).getEndId();
            visnodeS = getNodeById(nodes, eStartID);
            visnodeE = getNodeById(nodes, eEndID);
            deltaX = visnodeS.getX() - visnodeE.getX();
            deltaY = visnodeS.getY() - visnodeE.getY();
            deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY);

            double force = deltaLength > 0 ? deltaLength * deltaLength / k : 0;
            if (Double.isNaN(force)) {
                System.err.println("force is NaN edge is" + visnodeS.id + "->" + visnodeE.id);
            }
            double xDisp = (deltaX / deltaLength) * force;
            double yDisp = (deltaY / deltaLength) * force;
            dispX.put(eStartID, dispX.get(eStartID) - xDisp);
            dispY.put(eStartID, dispY.get(eStartID) - yDisp);
            dispX.put(eEndID, dispX.get(eEndID) + xDisp);
            dispY.put(eEndID, dispY.get(eEndID) + yDisp);
        }

        for (int v = 0; v < nodes.size(); v++) {
            deltaX = nodes.get(v).getX() - (double)(w / 2);
            deltaY = nodes.get(v).getY() - (double)(l / 2);
            deltaLength = Math.sqrt(deltaX * deltaX + deltaY * deltaY) - (visnodeS.getSize() + visnodeE.getSize());
            double force = deltaLength * deltaLength / (k * 20);
            double xDisp = (deltaX / deltaLength) * force;
            double yDisp = (deltaY / deltaLength) * force;
            dispX.put(nodes.get(v).getId(), dispX.get(nodes.get(v).getId()) - xDisp);
            dispY.put(nodes.get(v).getId(), dispY.get(nodes.get(v).getId()) - yDisp);
        }

        //set x,y
        for (int v = 0; v < nodes.size(); v++) {
            Node node = nodes.get(v);
            Double dx = dispX.get(node.getId());
            Double dy = dispY.get(node.getId());
            Double dispLength = Math.sqrt(dx * dx + dy * dy);
            double xDisp = dx / dispLength * Math.min(dispLength, temperature);
            double yDisp = dy / dispLength * Math.min(dispLength, temperature);

            // don't let nodes leave the display
            node.setX(node.getX()+xDisp);
            node.setY(node.getY()+yDisp);
            double random = 10 * new Random().nextDouble();
            node.setX(Math.min(w - random, Math.max(0 + random, node.getX())));
            node.setY(Math.min(l - random, Math.max(0 + random, node.getY())));
        }

        //cool temperature
        cool(curIter);
//        temperature*=0.95;
        return nodes;
    }

    private void cool(int curIter) {
        temperature *= (1.0 - curIter / (double) maxIter);
    }

    private Node getNodeById(List<Node> nodes, Long id) {
        for (Node node : nodes) {
            if (node.getId().equals(id)) {
                return node;
            }
        }
        return null;
    }
}
