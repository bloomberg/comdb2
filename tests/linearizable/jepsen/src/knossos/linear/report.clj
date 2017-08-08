(ns knossos.linear.report
  "Constructs reports from a linear analysis.

  When a linear analyzer terminates it gives us a set of configs, each of which
  consists of a model, a final operation, and a set of pending operations. Our
  job is to render those operations like so:

              +---------+ +--------+
      proc 0  | write 1 | | read 0 |
              +---------+ +--------+
                    +---------+
      proc 1        | write 0 |
                    +---------+

      ------------ time ---------->"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [clojure.string :as str]
            [knossos [history :as history]
                     [op :as op]
                     [core :as core]
                     [model :as model]]
            [analemma [xml :as xml]
                      [svg :as svg]]))

(def min-step "How small should we quantize the graph?" 1/10)

(def font "'Helvetica Neue', Helvetica, sans-serif")

(def process-height
  "How tall should an op be in process space?"
  0.4)

(def hscale- 150.0)

(defn hscale
  "Convert our units to horizontal CSS pixels"
  [x]
  (* x hscale-))

(defn vscale
  "Convert our units to vertical CSS pixels"
  [x]
  (* x 60.0))

(def stroke-width
  (vscale 0.05))

(def type->color
  {:ok   "#B3F3B5"
   nil   "#F2F3B3"
   :fail "#F3B3B3"})

(defn op-color
  "What color should an op be?"
  [pair-index op]
  (-> pair-index
      (history/completion op)
      :type
      type->color))

(defn transition-color
  "What color should a transition be?"
  [transition]
  (if (model/inconsistent? (:model transition))
    "#C51919"
    "#000000"))

(def faded "opacity" "0.15")

(def activate-script
  (str "<![CDATA[
function abar(id) {
  var bar = document.getElementById(id);
  bar.setAttribute('opacity', '1.0');
  var model = bar.getElementsByClassName('model')[0];
  if (model != undefined) {
    model.setAttribute('opacity', '1.0');
  }
}

function dbar(id) {
  var bar = document.getElementById(id);
  bar.setAttribute('opacity', '" faded "');
  var model = bar.getElementsByClassName('model')[0];
  if (model != undefined) {
    model.setAttribute('opacity', '0.0');
  }
}
]]>"))

(defn glow-filter
  []
  (svg/defs ["glow" [:filter {:x "0" :y "0"}
                     [:feGaussianBlur {:in "SourceAlpha"
                                       :stdDeviation "2"
                                       :result "blurred"}]
                     [:feFlood {:flood-color "#fff"}]
                     [:feComposite {:operator "in" :in2 "blurred"}]
                     [:feComponentTransfer
                      [:feFuncA {:type "linear" :slope "10" :intercept 0}]]
                     [:feMerge
                      [:feMergeNode]
                      [:feMergeNode {:in "SourceGraphic"}]]]]))


(defn set-attr
  "Set a single xml attribute"
  [elem attr value]
  (xml/set-attrs elem (assoc (xml/get-attrs elem) attr value)))

(defn ops
  "Computes distinct ops in an analysis."
  [analysis]
  (->> analysis
       :final-paths
       (mapcat (partial map :op))
       distinct))

(defn models
  "Computes distinct models in an analysis."
  [analysis]
  (->> analysis
       :final-paths
       (mapcat (partial map :model))
       distinct))

(defn model-numbers
  "A map which takes models and returns an integer."
  [models]
  (->> models (map-indexed (fn [i x] [x i])) (into {})))

(defn process-coords
  "Given a set of operations, computes a map of process identifiers to their
  track coordinates 0, 2, 4, ..."
  [ops]
  (->> ops
       history/processes
       history/sort-processes
       (map-indexed (fn [i process] [process i]))
       (into {})))

(defn time-bounds
  "Given a pair index and an analysis, computes the [lower, upper] bounds on
  times for rendering a plot."
  [pair-index analysis]
  [(dec (or (:index (history/invocation pair-index (:previous-op analysis)))
            1))
   (->> analysis
        :final-paths
        (mapcat (partial keep (fn [transition]
                                (->> transition
                                     :op
                                     (history/completion pair-index)
                                     :index))))
        (reduce max 0)
        inc)])

(defn condense-time-coords
  "Takes time coordinates (a map of op indices to [start-time end-time]), and
  condenses times to remove sparse regions."
  [coords]
  (let [mapping (->> coords
                     vals
                     (apply concat)
                     (into (sorted-set))
                     (map-indexed (fn [i coord] [coord i]))
                     (into {}))]
    (->> coords
         (map (fn [[k [t1 t2]]]
                [k [(mapping t1) (mapping t2)]]))
         (into {}))))

(defn time-coords
  "Takes a pair index, time bounds, and a set of ops. Returns a map of op
  indices to logical [start-time end-time] coordinates."
  [pair-index [tmin tmax] ops]
  (->> ops
       (map (fn [op]
              (let [i   (:index op)
                    _   (assert i)
                    t1  (max tmin (:index (history/invocation pair-index op)))
                    t2  (or (:index (history/completion pair-index op))
                            tmax)]
                [i [(- t1 tmin)
                    (- t2 tmin)]])))
       (into {})
       condense-time-coords))

(defn path-bounds
  "Assign an initial :y, :min-x and :max-x to every transition in a path."
  [{:keys [time-coords process-coords]} path]
  (map (fn [transition]
         (let [op      (:op transition)
               [t1 t2] (time-coords (:index op))]
           (assoc transition
                  :y (process-coords (:process op))
                  :min-x t1
                  :max-x t2)))
       path))

(defn paths
  "Given time coords, process coords, and an analysis, emits paths with
  coordinate bounds."
  [analysis time-coords process-coords]
  (->> analysis
       :final-paths
       (map (partial path-bounds {:time-coords    time-coords
                                  :process-coords process-coords}))))

(defn path->line
  "Takes a map of coordinates to models, path, a collection of lines, and emits
  a collection of lines with :min-x0, :max-x0, :y0, :min-x1, :max-x1, :y1
  coordinate ranges, and an attached model for the termination point. Assigns a
  :line-id to each transition for the line which terminates there. Returns a
  tuple [path' lines' bars'] where path' has line ids referring to indices in
  lines."
  [path lines models]
  (let [[path lines models] (reduce
                              (fn [[path lines models x0 y0] transition]
                                (let [y1 (:y transition)
                                      model (:model transition)
                                      x1 (loop [x1 (max (+ x0 min-step)
                                                        (:min-x transition))]
                                           (let [m (:model
                                                     (models {:x x1, :y y1}))]
                                             (if (and m (not= m model))
                                               ; Taken
                                               (recur (+ x1 min-step))
                                               x1)))
                                      bar (get models {:x x1 :y y1}
                                               {:model  model
                                                :id     (count models)})]
                                  (assert (<= x1 (:max-x transition))
                                          (str x1 " starting at " x0
                                               " is outside ["
                                               (:min-x transition)
                                               ", "
                                               (:max-x transition)
                                               "]\n"
                                               (with-out-str
                                                 (pprint path)
                                                 (pprint transition))))

                                  (if (nil? y0)
                                    ; First step
                                    [(conj path (assoc transition
                                                       :bar-id (:id bar)))
                                     lines
                                     (assoc models {:x x1 :y y1} bar)
                                     x1
                                     y1]
                                    ; Recurrence
                                    [(conj path (assoc transition
                                                       :line-id (count lines)
                                                       :bar-id  (:id bar)))
                                     (conj lines {:id      (count lines)
                                                  :model   (:model transition)
                                                  :x0      x0
                                                  :y0      y0
                                                  :x1      x1
                                                  :y1      y1})
                                     (assoc models {:x x1 :y y1} bar)
                                     x1
                                     y1])))
                              [[] lines models Double/NEGATIVE_INFINITY nil]
                              path)]
    [path lines models]))

(defn paths->initial-lines
  "Takes a collection of paths and returns:

  0. That collection of paths with each transition augmented with a :line-id
  1. A collection of lines indexed by line-id.
  2. A set of models, each with :x and :y coordinates."
  [paths]
  (reduce (fn [[paths lines models] path]
            (let [[path lines models] (path->line path lines models)]
              [(conj paths path) lines models]))
          [[] [] {}]
          paths))

(defn recursive-get
  "Looks up a key in a map recursively, by taking (get m k) and using it as a
  new key."
  [m k]
  (when-let [v (get m k)]
    (or (recursive-get m v)
        v)))

(defn collapse-mapping
  "Takes a map of x->x, where x may be a key in the map, and flattens it such
  that every key points directly to its final target."
  [m]
  (->> m
       keys
       (map (fn [k] [k (recursive-get m k)]))
       (into {})))

(defn merge-lines-r
  "Given [a set of lines, a mapping], and a group of overlapping lines, merges
  those lines, returning [lines mapping]."
  [[lines mapping] candidates]
  (if (empty? candidates)
    [lines mapping]
    ; Pretty easy; just drop every line but the first.
    (let [id0 (:id (first candidates))]
      (reduce (fn [[lines mapping] {:keys [id]}]
                [(assoc lines id nil)
                 (assoc mapping id id0)])
              [lines mapping]
              (next candidates)))))

(defn merge-lines
  "Takes an associative collection of line-ids to lines and produces a new
  collection of lines, and a map which takes the prior line IDs to new line
  IDs."
  [lines]
  (->> lines
       (group-by (juxt :x0 :y0 :x1 :y1 :model))
       vals
       (reduce merge-lines-r [lines {}])))

(defn paths->lines
  "Many path components are degenerate--they refer to equivalent state
  transitions. We want to map a set of paths into a smaller set of lines from
  operation to operation. We have coordinate bounds on every transition. Our
  job is to find non-empty intersections of those bounds for equivalent
  transitions and collapse them.

  We compute three data structures:

  - A collection of paths, each transition augmented with a numeric :line-id
  which identifies the line leading to that transition.
  - An indexed collection of line IDs to {:x0, y0, :x1, :y1} line segments
  connecting transitions.
  - A set of models, each with {:x :y :model} keys."
  [paths]
  (let [[paths lines models]  (paths->initial-lines paths)
        [lines mapping]       (merge-lines lines)
        lines           (into {} (map (juxt :id identity) (remove nil? lines)))
        mapping         (collapse-mapping mapping)
        paths           (map (fn [path]
                               (map (fn [transition]
                                      (if-let [id (mapping
                                                      (:line-id transition))]
                                        (assoc transition :line-id id)
                                        transition))
                                    path))
                               paths)]
    [paths lines models]))

(defn reachable
  "A map of an id (bar- or line-) to all ids for all paths that touch that id."
  [paths]
  (reduce (fn [rs path]
            (let [ids (mapcat (fn [transition]
                                (when-let [b (:bar-id transition)]
                                  (if-let [l (:line-id transition)]
                                    (list (str "line-" l) (str "bar-"  b))
                                    (list (str "bar-" b)))))
                              path)]
              (reduce (fn [rs id]
                        (assoc rs id (into (get rs id #{}) ids)))
                      rs
                      ids)))
          {}
          paths))

(defn coordinate-density
  "Construct a sorted map of coordinate regions to the maximum number of bars
  in a process in that region."
  [bars]
  (->> bars
       keys
       (map (fn [{:keys [x y]}] [(Math/floor x) y]))
       (group-by first)
       (map (fn [[x ys]]
              [x (->> ys
                      frequencies
                      vals
                      (reduce max 0))]))
       (into (sorted-map))))

(defn warp-time-coordinates
  "Often times there are dead spots or very dense spots in the time axis. We
  want to make the plot easier to read by compacting unused regions. Returns a
  function of times to warped times."
  [time-coords bars]
  (let [density     (coordinate-density bars)
        dmax        (->> density vals (reduce max))
        tmin        (->> time-coords vals flatten (reduce min))
        tmax        (->> time-coords vals flatten (reduce max))
        m (->> (range tmin (inc tmax) 1)
               ; Build [lower-time, scale] pairs.
               (map (fn [t]
                      (let [t (Math/floor t)
                            d (/ (get density t 1) dmax)]
                        [t d])))
               ; Transform to cumulative offsets and scales
               (reduce (fn [[pairs offset] [t d]]
                         [(conj pairs [t {:offset offset
                                          :scale  d}])
                          (+ offset d)])
                       [[] 0])
               ; Build a map of times to {offset, scale} maps
               first
               (into {}))]
    (fn [t]
      (let [{:keys [offset scale]} (m (Math/floor t))]
        (+ offset (* scale (mod t 1)))))))

(defn learnings
  "What a terrible function name. We should task someone with an action item to
  double-click down on this refactor.

  Basically we're taking an analysis and figuring out all the stuff we're gonna
  need to render it."
  [history analysis]
  (let [history         (history/index (history/complete history))
        pair-index      (history/pair-index history)
        ops             (ops analysis)
        models          (models analysis)
        model-numbers   (model-numbers models)
        process-coords  (process-coords ops)
        time-bounds     (time-bounds pair-index analysis)
        time-coords     (time-coords pair-index time-bounds ops)
        paths           (paths analysis time-coords process-coords )
        [paths lines bars] (paths->lines paths)
        reachable       (reachable paths)
        hscale          (comp hscale (warp-time-coordinates time-coords bars))]
    {:history       history
     :analysis      analysis
     :pair-index    pair-index
     :ops           ops
     :models        models
     :model-numbers model-numbers
     :process-coords process-coords
     :time-bounds   time-bounds
     :time-coords   time-coords
     :paths         paths
     :lines         lines
     :bars          bars
     :reachable     reachable
     :hscale        hscale}))

(defn render-ops
  "Given learnings, renders all operations as a group of SVG tags."
  [{:keys [hscale time-coords process-coords pair-index ops]}]
  (->> ops
       (mapv (fn [op]
              (let [[t1 t2] (time-coords    (:index op))
                    p       (process-coords (:process op))
                    width   (- (hscale t2) (hscale t1))] ; nonlinear coords
                (svg/group
                  (svg/rect (hscale t1)
                            (vscale p)
                            (vscale process-height)
                            width
                            :rx (vscale 0.1)
                            :ry (vscale 0.1)
                            :fill (op-color pair-index op))
                  (-> (svg/text (str (name (:f op)) " "
                                     (pr-str (:value op))))
                      (xml/add-attrs :x (+ (hscale t1) (/ width 2.0))
                                     :y (vscale (+ p (/ process-height
                                                        2.0))))
                      (svg/style :fill "#000000"
                                 :font-size (vscale (* process-height 0.6))
                                 :font-family font
                                 :alignment-baseline :middle
                                 :text-anchor :middle))))))
       (apply svg/group)))

(defn activate-line
  "On hover, highlights all related IDs for this element."
  [element reachable]
  (let [ids (->> (xml/get-attrs element)
                 :id
                 reachable
                 (map (fn [id] (str "'" id "'")))
                 (str/join ","))]
    (xml/add-attrs
      element
      :onmouseover (str "[" ids "].forEach(abar);")
      :onmouseout  (str "[" ids "].forEach(dbar);"))))

(defn outline-text
  [element]
  (xml/add-attrs element :filter "url(#glow)"))

(defn render-bars
  "Given learnings, renders all bars as a group of SVG tags."
  [{:keys [hscale bars reachable]}]
  (->> bars
       (map (fn [[{:keys [x y]} {:keys [id model] :as bar}]]
              (-> (svg/group
                    (-> (svg/line (hscale x) (vscale y)
                                  (hscale x) (vscale (+ y process-height))
                                  :stroke-width stroke-width
                                  :stroke       (transition-color bar)))
                    (-> (svg/text (str model))
                        (xml/add-attrs :class "model"
                                       :opacity "0.0"
                                       :x (hscale x)
                                       :y (vscale (- y 0.1)))
                        (svg/style :fill (transition-color bar)
                                   :font-size (vscale (* process-height 0.5))
                                   :font-family font
                                   :alignment-baseline :baseline
                                   :text-anchor :middle)
                        outline-text))
                  (xml/add-attrs :id (str "bar-" id)
                                 :opacity faded)
                  (activate-line reachable))))
       (apply svg/group)))

(defn render-lines
  "Given learnings, renders all lines as a group of SVG tags."
  [{:keys [hscale lines reachable]}]
  (->> lines
       vals
       (map (fn [{:keys [id x0 y0 x1 y1] :as line}]
               (let [up? (< y0 y1)
                     y0  (if up? (+ y0 process-height) y0)
                     y1  (if up? y1 (+ y1 process-height))]
                 (-> (svg/line (hscale x0) (vscale y0)
                               (hscale x1) (vscale y1)
                               :id            (str "line-" id)
                               :stroke-width  stroke-width
                               :stroke        (transition-color line)
                               :opacity       faded)
                     (activate-line reachable)))))
       (apply svg/group)))

(def legend-height
  (* process-height 0.6))

(defn legend-text
  [s x y & style]
  (apply svg/style
         (-> (svg/text s)
             (xml/add-attrs :x (hscale x)
                            :y (vscale y)))
         :fill "#666"
         :font-size (vscale legend-height)
         :font-family font
         style))

(defn render-process-legend
  "Process numbers and time arrow"
  [{:keys [process-coords]}]
  (->> process-coords
       (map (fn [[process y]]
              (legend-text (str process)
                           -0.1 (+ y (/ process-height 2.0))
                           :alignment-baseline :middle
                           :text-anchor :end)))
       (apply svg/group)))

(defn render-legend
  [learnings]
  (let [y (->> learnings :process-coords vals (reduce max 0)
               (+ process-height 0.5))
        nonlinear-hscale (:hscale learnings)
        xmax   (/ (->> learnings :time-coords vals (map second)
                       (reduce max 0) nonlinear-hscale)
                  hscale-)]
    (svg/group
      (legend-text "Process" -0.1 y :alignment-baseline :baseline :text-anchor :end)
      (legend-text "Time ─────▶"
                   0 y
                   :alignment-baseline :baseline
                   :text-anchor :start)

      (legend-text "Legal"
                   (- xmax 2.2) y
                   :alignment-baseline :baseline
                   :text-anchor :end)
      (svg/line (hscale (- xmax 2.15))
                (vscale (+ y (* legend-height -0.3)))
                (hscale (- xmax 2.05))
                (vscale (+ y (* legend-height -0.3)))
                :stroke-width stroke-width
                :stroke (transition-color nil))

      (legend-text "Illegal"
                   (- xmax 1.65) y
                   :alignment-baseline :baseline
                   :text-anchor :end)
      (svg/line (hscale (- xmax 1.6))
                (vscale (+ y (* legend-height -0.3)))
                (hscale (- xmax 1.5))
                (vscale (+ y (* legend-height -0.3)))
                :stroke-width stroke-width
                :stroke (transition-color {:model (model/inconsistent nil)}))

      (legend-text "Crashed Op"
                   (- xmax 0.85) y
                   :alignment-baseline :baseline
                   :text-anchor :end)
      (svg/rect (hscale (- xmax 0.8))
                (vscale (- y legend-height -0.03))
                (vscale legend-height)
                (hscale 0.16)
                :rx (vscale 0.05)
                :ry (vscale 0.05)
                :fill (type->color nil))

      (legend-text "OK Op"
                   (- xmax 0.21) y
                   :alignment-baseline :baseline
                   :text-anchor :end)
      (svg/rect (hscale (- xmax 0.16))
                (vscale (- y legend-height -0.03))
                (vscale legend-height)
                (hscale 0.16)
                :rx (vscale 0.05)
                :ry (vscale 0.05)
                :fill (type->color :ok))
      (render-process-legend learnings))))

(defn svg-2
  "Emits an SVG 2 document."
  [& args]
  (-> (apply svg/svg args)
      (set-attr "version" "2.0")))

(defn render-analysis!
  "Render an entire analysis."
  [history analysis file]
  (let [learnings  (learnings history analysis)
        ops        (render-ops   learnings)
        bars       (render-bars  learnings)
        lines      (render-lines learnings)
        legend     (render-legend learnings)]
    (spit file
          (xml/emit
            (svg-2
              [:script {:type "application/ecmascript"} activate-script]
              (glow-filter)
              (-> (svg/group
                    lines
                    ops
                    bars
                    legend)
                  (svg/translate (vscale 1.4) (vscale 0.4))))))))
