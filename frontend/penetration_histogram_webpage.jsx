import { useEffect, useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

export default function PenetrationHistogram() {
  const [data, setData] = useState([]);
  const wsRef = useRef(null);

  const connect = () => {
    if (wsRef.current) wsRef.current.close();
    const ws = new WebSocket("ws://127.0.0.1:3000/ws");
    wsRef.current = ws;

    ws.onopen = () => {
      ws.send(
        JSON.stringify({
          exchange: "bitmex",
          cmd: { Subscribe: { channel: "Book", symbol: "XBTUSDT" } },
        })
      );
      ws.send(
        JSON.stringify({
          exchange: "bitmex",
          cmd: { Subscribe: { channel: "Trade", symbol: "XBTUSDT" } },
        })
      );
    };

    ws.onmessage = (msg) => {
      try {
        const parsed = JSON.parse(msg.data);
        if (parsed.Penetration) {
          const counts = parsed.Penetration.counts;
          const chartData = counts.map((v, i) => ({ level: i, value: v }));
          setData(chartData);
        }
      } catch (e) {
        console.error("Invalid JSON", e);
      }
    };
  };

  return (
    <div className="p-6 space-y-4">
      <Button onClick={connect}>Connect WebSocket</Button>

      <Card className="w-full h-[500px]">
        <CardContent className="w-full h-full p-4">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <XAxis dataKey="level" hide={true} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="value" />
            </BarChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>
    </div>
  );
}
