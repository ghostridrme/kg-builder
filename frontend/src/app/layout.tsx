import type { Metadata } from "next";
import "./globals.css";
export const metadata: Metadata = {
  title: "KG Pipeline · Graph explorer",
  description: "Explore infrastructure, applications and their dependencies",
};
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
